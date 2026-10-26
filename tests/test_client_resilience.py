"""
Download-resilience tests for the sync/async remote clients.

A tiny in-process ASGI app serves a completed job whose artifact stream
either breaks mid-transfer ("raise") or lies about Content-Length while
sending a truncated body ("trunc"). These mirror a real network drop and
a cut-off response. Both clients must:

* raise RemoteTranscriberError,
* leave the previous complete round byte-for-byte in place,
* leave no .part / staging residue,
* succeed on retry.

Also covers stale managed-file removal over the remote path and direct
unit checks of the per-file verification helper.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from transcribe_anything import client as client_mod
from transcribe_anything.client import (
    RemoteTranscriberError,
    _verify_downloaded,
    transcribe_remote,
    transcribe_remote_async,
)
from transcribe_anything.publish import list_managed_artifacts

CORE = ["out.txt", "out.srt", "out.vtt", "out.json"]
JOB_ID = "j1"
ARTIFACTS = sorted(CORE)

NEW_CONTENT = {
    "out.txt": b"new:txt",
    "out.srt": b"1\n00:00:00,000 --> 00:00:01,000\nnew subtitle\n",
    "out.vtt": b"WEBVTT\n\nnew vtt\n",
    "out.json": json.dumps({"text": "new"}).encode("utf-8"),
}


def _seed_old_round(out_dir: Path, *, with_mp4: bool = False) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in CORE:
        (out_dir / name).write_bytes(f"old:{name}".encode())
    if with_mp4:
        (out_dir / "out.mp4").write_bytes(b"old-mp4")


def _make_app(mode: str) -> Starlette:
    """mode: 'healthy' | 'raise' | 'trunc'."""

    def submit(request):
        return JSONResponse({"job_id": JOB_ID})

    def job(request):
        return JSONResponse({"status": "completed", "artifacts": ARTIFACTS})

    async def artifact(request):
        name = request.path_params["name"]
        body = NEW_CONTENT[name]
        if name == "out.srt" and mode == "raise":

            async def broken_stream():
                yield body[:3]
                raise RuntimeError("connection lost")

            return StreamingResponse(broken_stream(), media_type="text/plain")
        if name == "out.srt" and mode == "trunc":

            async def trunc_stream():
                yield body[:3]  # generator stops early; declared length lies

            return StreamingResponse(
                trunc_stream(),
                headers={"content-length": str(len(body))},
                media_type="text/plain",
            )
        return Response(body, media_type="application/octet-stream")

    return Starlette(
        routes=[
            Route("/v1/transcribe", submit, methods=["POST"]),
            Route(f"/v1/jobs/{JOB_ID}", job),
            Route(f"/v1/jobs/{JOB_ID}/artifacts/{{name}}", artifact),
        ]
    )


def _patch_sync_client(monkeypatch, app: Starlette) -> None:
    def factory(*args, **kwargs):
        kwargs.pop("timeout", None)
        return TestClient(app, base_url="http://testserver", headers=kwargs.get("headers") or {})

    monkeypatch.setattr(client_mod.httpx, "Client", factory)


def _patch_async_client(monkeypatch, app: Starlette) -> None:
    transport = httpx.ASGITransport(app=app)
    real_async_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        kwargs.setdefault("base_url", "http://testserver")
        return real_async_client(*args, **kwargs)

    monkeypatch.setattr(client_mod.httpx, "AsyncClient", factory)


def _assert_old_round_intact(out_dir: Path, *, with_mp4: bool = False) -> None:
    expected = CORE + (["out.mp4"] if with_mp4 else [])
    assert list_managed_artifacts(out_dir) == sorted(expected)
    for name in CORE:
        assert (out_dir / name).read_bytes() == f"old:{name}".encode()
    if with_mp4:
        assert (out_dir / "out.mp4").read_bytes() == b"old-mp4"
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())


@pytest.mark.parametrize("mode,fragment", [("raise", "interrupted"), ("trunc", "truncated")])
def test_sync_interrupted_download_preserves_old_round_and_retries(monkeypatch, tmp_path, mode, fragment) -> None:
    out_dir = tmp_path / "out"
    _seed_old_round(out_dir)

    _patch_sync_client(monkeypatch, _make_app(mode))
    with pytest.raises(RemoteTranscriberError) as exc:
        transcribe_remote(
            url_or_file="https://example.com/foo.mp3",
            remote="http://testserver",
            output_dir=str(out_dir),
            poll_interval_seconds=0.01,
        )
    assert fragment in str(exc.value)
    _assert_old_round_intact(out_dir)

    _patch_sync_client(monkeypatch, _make_app("healthy"))
    result = transcribe_remote(
        url_or_file="https://example.com/foo.mp3",
        remote="http://testserver",
        output_dir=str(out_dir),
        poll_interval_seconds=0.01,
    )
    assert Path(result).resolve() == out_dir.resolve()
    assert list_managed_artifacts(out_dir) == sorted(CORE)
    for name, body in NEW_CONTENT.items():
        assert (out_dir / name).read_bytes() == body
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())


@pytest.mark.parametrize("mode,fragment", [("raise", "interrupted"), ("trunc", "truncated")])
def test_async_interrupted_download_preserves_old_round_and_retries(monkeypatch, tmp_path, mode, fragment) -> None:
    out_dir = tmp_path / "out"
    _seed_old_round(out_dir)

    _patch_async_client(monkeypatch, _make_app(mode))
    with pytest.raises(RemoteTranscriberError) as exc:
        asyncio.run(
            transcribe_remote_async(
                url_or_file="https://example.com/foo.mp3",
                remote="http://testserver",
                output_dir=str(out_dir),
                poll_interval_seconds=0.01,
            )
        )
    assert fragment in str(exc.value)
    _assert_old_round_intact(out_dir)

    _patch_async_client(monkeypatch, _make_app("healthy"))
    result = asyncio.run(
        transcribe_remote_async(
            url_or_file="https://example.com/foo.mp3",
            remote="http://testserver",
            output_dir=str(out_dir),
            poll_interval_seconds=0.01,
        )
    )
    assert Path(result).resolve() == out_dir.resolve()
    assert list_managed_artifacts(out_dir) == sorted(CORE)
    for name, body in NEW_CONTENT.items():
        assert (out_dir / name).read_bytes() == body
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())


def test_sync_remote_removes_stale_managed_files(monkeypatch, tmp_path) -> None:
    out_dir = tmp_path / "out"
    _seed_old_round(out_dir, with_mp4=True)

    _patch_sync_client(monkeypatch, _make_app("healthy"))
    transcribe_remote(
        url_or_file="https://example.com/foo.mp3",
        remote="http://testserver",
        output_dir=str(out_dir),
        poll_interval_seconds=0.01,
    )
    assert set(list_managed_artifacts(out_dir)) == set(CORE)
    assert not (out_dir / "out.mp4").exists()
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())


def test_verify_downloaded_rejects_empty_and_truncated(tmp_path: Path) -> None:
    part = tmp_path / "out.srt.part"
    part.write_bytes(b"")
    with pytest.raises(RemoteTranscriberError, match="empty"):
        _verify_downloaded(part, "out.srt", 0, 0)
    with pytest.raises(RemoteTranscriberError, match="truncated"):
        _verify_downloaded(part, "out.srt", 2, 100)
    # No Content-Length -> only emptiness is enforced (streamed servers).
    part.write_bytes(b"ok")
    _verify_downloaded(part, "out.srt", 2, None)


class _FirstArtifactBarrier:
    """Release both downloads only once both are streaming the first file.

    This makes reentrancy deterministic (no wall-clock sleeps): by the
    time the barrier opens, both coroutines are inside their own
    ``staged_round`` on one thread, so the second lock acquisition is
    genuinely reentrant.
    """

    def __init__(self, required: int = 2) -> None:
        self._required = required
        self._seen = 0
        self._ready = asyncio.Event()

    async def wait_in_stream(self) -> None:
        self._seen += 1
        if self._seen >= self._required:
            self._ready.set()
        await self._ready.wait()


def test_concurrent_async_downloads_same_directory(monkeypatch, tmp_path) -> None:
    """Two same-event-loop downloads targeting one directory both succeed.

    They run on one thread, so the per-directory lock is entered
    reentrantly; the later entry must not recovery-sweep the earlier
    live staging. The barrier guarantees both are mid-download (hence
    both hold a staging context) before any body is sent — under the old
    behavior the later entry had already swept the earlier staging and
    one task failed.
    """
    out_dir = tmp_path / "out"
    _seed_old_round(out_dir)
    holder: dict[str, _FirstArtifactBarrier] = {}

    def submit(request):
        return JSONResponse({"job_id": JOB_ID})

    def job(request):
        return JSONResponse({"status": "completed", "artifacts": ARTIFACTS})

    async def artifact(request):
        name = request.path_params["name"]
        body = NEW_CONTENT[name]

        async def chunks():
            await holder["barrier"].wait_in_stream()
            for i in range(0, len(body), 2):
                yield body[i : i + 2]
                await asyncio.sleep(0)  # yield: keep the two tasks interleaved

        return StreamingResponse(chunks(), media_type="application/octet-stream")

    app = Starlette(
        routes=[
            Route("/v1/transcribe", submit, methods=["POST"]),
            Route(f"/v1/jobs/{JOB_ID}", job),
            Route(f"/v1/jobs/{JOB_ID}/artifacts/{{name}}", artifact),
        ]
    )
    _patch_async_client(monkeypatch, app)

    async def _two() -> list[str]:
        holder["barrier"] = _FirstArtifactBarrier(2)
        return await asyncio.gather(
            transcribe_remote_async(
                url_or_file="https://example.com/a.mp3",
                remote="http://testserver",
                output_dir=str(out_dir),
                poll_interval_seconds=0.01,
            ),
            transcribe_remote_async(
                url_or_file="https://example.com/b.mp3",
                remote="http://testserver",
                output_dir=str(out_dir),
                poll_interval_seconds=0.01,
            ),
        )

    results = asyncio.run(_two())
    assert {Path(r).resolve() for r in results} == {out_dir.resolve()}
    assert list_managed_artifacts(out_dir) == sorted(CORE)
    for name, body in NEW_CONTENT.items():
        assert (out_dir / name).read_bytes() == body
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())


def test_publish_failure_is_surfaced_as_remote_error(monkeypatch, tmp_path) -> None:
    """An OSError while publishing a downloaded round is wrapped, never raw."""
    from transcribe_anything.publish import staged_round

    with staged_round(tmp_path) as stage:
        for name, body in NEW_CONTENT.items():
            (stage.path / name).write_bytes(body)

        def _boom(_stage) -> None:
            raise OSError("staging vanished")

        monkeypatch.setattr(client_mod, "commit_round", _boom)
        with pytest.raises(RemoteTranscriberError, match="failed to publish downloaded round"):
            client_mod._publish_downloaded_round(stage, CORE)
