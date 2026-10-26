"""
HTTP client used by the ``--remote`` CLI / Python-API path.

The client mirrors the local :func:`transcribe_anything.api.transcribe`
contract: the caller hands over a URL or file path and (eventually) gets
the same ``out.txt``/``out.srt``/``out.vtt``/``out.json`` artifact layout
in their ``output_dir``.

Uses ``httpx`` (synchronous client). ``httpx`` is in the base package
deps so client mode works out of the box.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import httpx

from transcribe_anything.publish import (
    ArtifactValidationError,
    StagedRound,
    commit_round,
    staged_round,
    validate_staging,
)


class RemoteTranscriberError(Exception):
    """Raised when a remote daemon rejects or fails a transcription request."""


def _build_headers(token: Optional[str]) -> dict:
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def _collect_options(
    *,
    model: Optional[str],
    task: Optional[str],
    language: Optional[str],
    initial_prompt: Optional[str],
    align: bool,
    align_model: Optional[str],
    other_args: Optional[list[str]],
    embed: bool,
) -> dict:
    options: dict[str, Any] = {}
    if model is not None:
        options["model"] = model
    if task:
        options["task"] = task
    if language:
        options["language"] = language
    if initial_prompt:
        options["initial_prompt"] = initial_prompt
    if align:
        options["align"] = True
    if align_model:
        options["align_model"] = align_model
    if other_args:
        options["other_args"] = list(other_args)
    if embed:
        options["embed"] = True
    return options


# ---------------------------------------------------------------------------
# Artifact download staging (shared by sync and async clients)
# ---------------------------------------------------------------------------


def _expected_content_length(response: Any) -> int | None:
    raw = response.headers.get("content-length")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _verify_downloaded(part: Path, name: str, total: int, expected_length: int | None) -> None:
    """Validate one finished artifact download before it leaves staging."""
    if total == 0:
        raise RemoteTranscriberError(f"downloaded empty artifact: {name}")
    if expected_length is not None and total != expected_length:
        raise RemoteTranscriberError(f"download truncated for artifact {name}: got {total} bytes, " f"expected {expected_length}")


def _publish_downloaded_round(stage: StagedRound, expected_names: list) -> None:
    """Validate the downloaded set and atomically publish it.

    Uses the exact same staging / validate / commit protocol as local
    transcription runs, so stale managed files are removed and user
    files are preserved identically on both paths.
    """
    try:
        staged_names = {p.name for p in stage.path.iterdir() if p.is_file()}
        expected = set(expected_names)
        if staged_names != expected:
            raise RemoteTranscriberError("downloaded artifact set mismatch: " f"missing={sorted(expected - staged_names)}, " f"extra={sorted(staged_names - expected)}")
        validate_staging(stage)
        commit_round(stage)
    except ArtifactValidationError as exc:
        raise RemoteTranscriberError(str(exc)) from exc
    except OSError as exc:
        # Staging vanished or a replace/unlink failed mid-publish; the
        # staging context sweeps the residue and the previous complete
        # round stays in place. Keep the remote contract: never leak a
        # raw filesystem error to the caller.
        raise RemoteTranscriberError(f"failed to publish downloaded round: {exc}") from exc


def _download_artifact_sync(client: httpx.Client, base_url: str, job_id: str, name: str, stage: StagedRound) -> None:
    """Stream one artifact into staging (sync client), via a ``.part`` name."""
    part = stage.path / f"{name}.part"
    total = 0
    expected_length: int | None = None
    try:
        with client.stream("GET", f"{base_url}/v1/jobs/{job_id}/artifacts/{name}") as r:
            if r.status_code >= 400:
                raise RemoteTranscriberError(f"failed to download artifact {name}: {r.status_code} {r.text}")
            expected_length = _expected_content_length(r)
            with part.open("wb") as fh:
                for chunk in r.iter_bytes():
                    fh.write(chunk)
                    total += len(chunk)
    except RemoteTranscriberError:
        raise
    except (httpx.HTTPError, OSError, RuntimeError) as exc:
        # The boundary covers iteration *and* stream close: transports
        # surface socket/framing errors (httpx.HTTPError/OSError), and
        # in-process test portals surface the server-side break as a
        # RuntimeError when the stream context exits.
        raise RemoteTranscriberError(f"download interrupted for artifact {name}: {exc}") from exc
    _verify_downloaded(part, name, total, expected_length)
    os.replace(part, stage.path / name)


async def _download_artifact_async(client: httpx.AsyncClient, base_url: str, job_id: str, name: str, stage: StagedRound) -> None:
    """Stream one artifact into staging (async client), via a ``.part`` name."""
    part = stage.path / f"{name}.part"
    total = 0
    expected_length: int | None = None
    try:
        async with client.stream("GET", f"{base_url}/v1/jobs/{job_id}/artifacts/{name}") as r:
            if r.status_code >= 400:
                raise RemoteTranscriberError(f"failed to download artifact {name}: {r.status_code} {await r.aread()!r}")
            expected_length = _expected_content_length(r)
            with part.open("wb") as fh:
                async for chunk in r.aiter_bytes():
                    fh.write(chunk)
                    total += len(chunk)
    except RemoteTranscriberError:
        raise
    except (httpx.HTTPError, OSError, RuntimeError) as exc:
        # See the sync downloader: a failure up to and including stream
        # close aborts the round; the staging context sweeps the .part.
        raise RemoteTranscriberError(f"download interrupted for artifact {name}: {exc}") from exc
    _verify_downloaded(part, name, total, expected_length)
    os.replace(part, stage.path / name)


def transcribe_remote(
    url_or_file: str,
    *,
    remote: str,
    output_dir: Optional[str] = None,
    token: Optional[str] = None,
    model: Optional[str] = None,
    task: Optional[str] = None,
    language: Optional[str] = None,
    initial_prompt: Optional[str] = None,
    align: bool = False,
    align_model: Optional[str] = None,
    other_args: Optional[list[str]] = None,
    embed: bool = False,
    poll_interval_seconds: float = 1.0,
    request_timeout_seconds: float = 30.0,
    job_timeout_seconds: float = 60 * 60 * 4,
) -> str:
    """Submit a transcription job to a remote daemon and download artifacts locally.

    Returns the absolute path to the output directory, matching the shape
    of :func:`transcribe_anything.api.transcribe`.
    """
    base_url = _normalize_base_url(remote)
    headers = _build_headers(token)
    options = _collect_options(
        model=model,
        task=task,
        language=language,
        initial_prompt=initial_prompt,
        align=align,
        align_model=align_model,
        other_args=other_args,
        embed=embed,
    )

    # Decide submission mode: URL or multipart file upload.
    is_url = url_or_file.startswith("http://") or url_or_file.startswith("https://") or url_or_file.startswith("ftp://")

    client = httpx.Client(timeout=request_timeout_seconds, headers=headers)
    try:
        if is_url:
            payload = {"url": url_or_file, **options}
            resp = client.post(f"{base_url}/v1/transcribe", json=payload)
        else:
            local = Path(url_or_file)
            if not local.is_file():
                raise RemoteTranscriberError(f"local file not found: {url_or_file}")
            with local.open("rb") as fh:
                files = {"file": (local.name, fh, "application/octet-stream")}
                data = {"options": json.dumps(options)} if options else {}
                resp = client.post(f"{base_url}/v1/transcribe", files=files, data=data)
        if resp.status_code >= 400:
            raise RemoteTranscriberError(f"daemon at {base_url} rejected submission: {resp.status_code} {resp.text}")
        body = resp.json()
        job_id = body["job_id"]

        # Poll until terminal.
        deadline = time.time() + job_timeout_seconds
        last_status = None
        while True:
            if time.time() > deadline:
                raise RemoteTranscriberError(f"job {job_id} timed out after {job_timeout_seconds}s")
            jr = client.get(f"{base_url}/v1/jobs/{job_id}")
            if jr.status_code >= 400:
                raise RemoteTranscriberError(f"daemon returned {jr.status_code} fetching job status: {jr.text}")
            job = jr.json()
            status = job.get("status")
            if status != last_status:
                sys.stderr.write(f"remote job {job_id}: {status}\n")
                last_status = status
            if status == "completed":
                break
            if status == "failed":
                raise RemoteTranscriberError(f"daemon job {job_id} failed: {job.get('error')}")
            time.sleep(poll_interval_seconds)

        # Resolve output_dir similarly to api.transcribe's logic.
        if output_dir is None:
            base = Path(url_or_file).name
            stem = Path(base).stem if not is_url else (base or "remote")
            output_dir = f"text_{stem or 'remote'}"
        out_path = Path(output_dir).resolve()

        artifacts = job.get("artifacts") or []
        if not artifacts:
            raise RemoteTranscriberError(f"daemon job {job_id} reported no artifacts")
        # Download into a hidden staging directory, validate the complete
        # set, then atomically publish. An interrupted download leaves
        # the previous complete round in out_path untouched.
        with staged_round(out_path) as stage:
            for name in artifacts:
                _download_artifact_sync(client, base_url, job_id, name, stage)
            _publish_downloaded_round(stage, artifacts)
        return str(out_path)
    finally:
        client.close()


async def transcribe_remote_async(
    url_or_file: str,
    *,
    remote: str,
    output_dir: Optional[str] = None,
    token: Optional[str] = None,
    model: Optional[str] = None,
    task: Optional[str] = None,
    language: Optional[str] = None,
    initial_prompt: Optional[str] = None,
    align: bool = False,
    align_model: Optional[str] = None,
    other_args: Optional[list[str]] = None,
    embed: bool = False,
    poll_interval_seconds: float = 1.0,
    request_timeout_seconds: float = 30.0,
    job_timeout_seconds: float = 60 * 60 * 4,
) -> str:
    """Async mirror of :func:`transcribe_remote`.

    Same contract as the sync variant: submit a job, poll until terminal,
    download artifacts into ``output_dir``, return the path. Uses
    ``httpx.AsyncClient`` and ``asyncio.sleep`` so callers can drive
    many remote transcriptions concurrently without burning a thread per
    job.
    """
    base_url = _normalize_base_url(remote)
    headers = _build_headers(token)
    options = _collect_options(
        model=model,
        task=task,
        language=language,
        initial_prompt=initial_prompt,
        align=align,
        align_model=align_model,
        other_args=other_args,
        embed=embed,
    )

    is_url = url_or_file.startswith("http://") or url_or_file.startswith("https://") or url_or_file.startswith("ftp://")

    async with httpx.AsyncClient(timeout=request_timeout_seconds, headers=headers) as client:
        if is_url:
            payload = {"url": url_or_file, **options}
            resp = await client.post(f"{base_url}/v1/transcribe", json=payload)
        else:
            local = Path(url_or_file)
            if not local.is_file():
                raise RemoteTranscriberError(f"local file not found: {url_or_file}")
            with local.open("rb") as fh:
                files = {"file": (local.name, fh, "application/octet-stream")}
                data = {"options": json.dumps(options)} if options else {}
                resp = await client.post(f"{base_url}/v1/transcribe", files=files, data=data)
        if resp.status_code >= 400:
            raise RemoteTranscriberError(f"daemon at {base_url} rejected submission: {resp.status_code} {resp.text}")
        body = resp.json()
        job_id = body["job_id"]

        deadline = time.time() + job_timeout_seconds
        last_status = None
        while True:
            if time.time() > deadline:
                raise RemoteTranscriberError(f"job {job_id} timed out after {job_timeout_seconds}s")
            jr = await client.get(f"{base_url}/v1/jobs/{job_id}")
            if jr.status_code >= 400:
                raise RemoteTranscriberError(f"daemon returned {jr.status_code} fetching job status: {jr.text}")
            job = jr.json()
            status = job.get("status")
            if status != last_status:
                sys.stderr.write(f"remote job {job_id}: {status}\n")
                last_status = status
            if status == "completed":
                break
            if status == "failed":
                raise RemoteTranscriberError(f"daemon job {job_id} failed: {job.get('error')}")
            await asyncio.sleep(poll_interval_seconds)

        if output_dir is None:
            base = Path(url_or_file).name
            stem = Path(base).stem if not is_url else (base or "remote")
            output_dir = f"text_{stem or 'remote'}"
        out_path = Path(output_dir).resolve()

        artifacts = job.get("artifacts") or []
        if not artifacts:
            raise RemoteTranscriberError(f"daemon job {job_id} reported no artifacts")
        # Same staging/validate/publish protocol as the sync client.
        with staged_round(out_path) as stage:
            for name in artifacts:
                await _download_artifact_async(client, base_url, job_id, name, stage)
            _publish_downloaded_round(stage, artifacts)
        return str(out_path)


def resolve_remote_and_token(
    *,
    remote_arg: Optional[str],
    token_arg: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """Combine CLI flags + env vars to determine the active remote URL and token."""
    remote = remote_arg or os.environ.get("TRANSCRIBE_ANYTHING_REMOTE") or None
    token = token_arg or os.environ.get("TRANSCRIBE_ANYTHING_TOKEN") or None
    return remote, token


# ---------------------------------------------------------------- streaming


def _http_to_ws_url(url: str) -> str:
    """Normalize an http(s):// or ws(s):// daemon URL to its ws variant."""
    base = url.rstrip("/")
    if base.startswith("http://"):
        base = "ws://" + base[len("http://") :]
    elif base.startswith("https://"):
        base = "wss://" + base[len("https://") :]
    return base


async def stream_remote(
    audio_iter,
    *,
    remote: str,
    token: Optional[str] = None,
    model: Optional[str] = None,
    language: Optional[str] = None,
    sample_rate: int = 16000,
    on_event=None,
):
    """Stream PCM16-LE chunks from ``audio_iter`` to the daemon's ``WS /v1/stream``.

    ``audio_iter`` may be either a regular iterable / generator or an
    async iterable / async generator of ``bytes`` chunks. Each event
    from the server (``partial`` / ``final`` / ``metrics`` / ``done`` /
    ``error``) is forwarded to ``on_event(event_dict)`` if provided, and
    also yielded so callers can ``async for`` over the stream.

    Raises :class:`RemoteTranscriberError` if the daemon rejects the
    connection or emits an ``error`` frame.
    """
    import json as _json

    try:
        from websockets.asyncio.client import (
            connect as _ws_connect,  # type: ignore[import-not-found]
        )
    except ImportError as exc:  # pragma: no cover - exercised only without websockets
        raise RemoteTranscriberError("websockets is required for --stream-in: pip install websockets") from exc

    ws_url = _http_to_ws_url(remote) + "/v1/stream"
    headers = []
    if token:
        headers.append(("Authorization", f"Bearer {token}"))

    hello = {
        "type": "hello",
        "model": model or "small.en",
        "language": language,
        "sample_rate": sample_rate,
        "encoding": "pcm16le",
    }

    async with _ws_connect(ws_url, additional_headers=headers or None) as ws:
        await ws.send(_json.dumps(hello))
        first = await ws.recv()
        first_evt = _json.loads(first)
        if first_evt.get("type") == "error":
            raise RemoteTranscriberError(f"daemon rejected stream: {first_evt}")
        if first_evt.get("type") != "ready":
            raise RemoteTranscriberError(f"unexpected first event from daemon: {first_evt}")

        import asyncio as _asyncio

        async def _push_audio():
            if hasattr(audio_iter, "__aiter__"):
                async for chunk in audio_iter:
                    if not chunk:
                        break
                    await ws.send(chunk)
            else:
                for chunk in audio_iter:
                    if not chunk:
                        break
                    await ws.send(chunk)
            # Graceful EOF — daemon flushes outstanding partials then sends `done`.
            await ws.send(_json.dumps({"type": "end_of_input"}))

        pusher = _asyncio.create_task(_push_audio())
        try:
            async for raw in ws:
                evt = _json.loads(raw)
                if on_event is not None:
                    on_event(evt)
                yield evt
                if evt.get("type") == "done":
                    break
                if evt.get("type") == "error":
                    raise RemoteTranscriberError(f"daemon error: {evt}")
        finally:
            pusher.cancel()
            try:
                await pusher
            except (BaseException,):
                pass


def stream_pcm_from_stdin(
    *,
    remote: str,
    token: Optional[str] = None,
    model: Optional[str] = None,
    language: Optional[str] = None,
    chunk_bytes: int = 3200,  # 100ms @ 16kHz s16le mono
) -> None:
    """Sync entry point for ``transcribe-anything --remote ws://… --stream-in``.

    Reads PCM16-LE bytes from stdin, streams them to the daemon, prints
    transcript text to stdout as ``partial`` / ``final`` events arrive.
    Blocks until the input pipe closes and the daemon sends ``done``.
    """
    import asyncio as _asyncio
    import sys as _sys

    def _stdin_chunks():
        buf = _sys.stdin.buffer
        while True:
            chunk = buf.read(chunk_bytes)
            if not chunk:
                return
            yield chunk

    async def _run():
        last_locked_end = 0  # length of confirmed-final text so far
        agen = stream_remote(
            _stdin_chunks(),
            remote=remote,
            token=token,
            model=model,
            language=language,
        )
        async for evt in agen:
            kind = evt.get("type")
            if kind == "partial":
                _sys.stdout.write(f"\r[partial] {evt.get('text', '')}\033[K")
                _sys.stdout.flush()
            elif kind == "final":
                _sys.stdout.write(f"\r[final]   {evt.get('text', '')}\n")
                _sys.stdout.flush()
                last_locked_end += len(evt.get("text", ""))
            elif kind == "error":
                raise RemoteTranscriberError(f"daemon error: {evt}")
        _sys.stdout.write("\n")
        _sys.stdout.flush()

    _asyncio.run(_run())
