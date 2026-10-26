"""
Integration tests for staging / atomic publishing through ``api.transcribe``.

The Whisper backend, audio fetch and ffmpeg burn are stubbed so the tests
exercise the full transcribe() control flow without a model or GPU.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from transcribe_anything import api
from transcribe_anything.publish import list_managed_artifacts

CORE = ["out.txt", "out.srt", "out.vtt", "out.json"]


def _write_core(d: Path, tag: str) -> None:
    for ext in ("txt", "srt", "vtt", "json"):
        (d / f"clip.wav.{ext}").write_bytes(f"{tag}:{ext}".encode())


def _install_stubs(monkeypatch, fake_backend) -> None:
    monkeypatch.setattr(api, "fetch_audio", lambda url_or_file, tmp_wav: None)
    monkeypatch.setattr(api, "run_whisper", fake_backend)
    monkeypatch.setattr(api.shutil, "which", lambda name: "/usr/bin/static_ffmpeg")


def _make_media(tmp_path: Path) -> Path:
    media = tmp_path / "media.mp4"
    media.write_bytes(b"fake media")
    return media


def test_transcribe_publishes_complete_round_and_keeps_return_path(tmp_path: Path, monkeypatch) -> None:
    def fake_backend(input_wav, device, model, output_dir, task, language, other_args) -> None:
        _write_core(Path(output_dir), "new")

    _install_stubs(monkeypatch, fake_backend)
    media = _make_media(tmp_path)
    out_dir = tmp_path / "text_media"

    result = api.transcribe(url_or_file=str(media), output_dir=str(out_dir), model="tiny", device="cpu")

    assert result == os.path.abspath(str(out_dir))
    assert list_managed_artifacts(out_dir) == sorted(CORE)
    assert (out_dir / "out.srt").read_bytes() == b"new:srt"
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())


def test_new_products_are_invisible_until_commit(tmp_path: Path, monkeypatch) -> None:
    observed: dict[str, object] = {}

    def fake_backend(input_wav, device, model, output_dir, task, language, other_args) -> None:
        staging = Path(output_dir)
        final_dir = staging.parent
        _write_core(staging, "new")
        # Snapshot taken while the backend is still writing: no new round
        # product may be visible in the final directory yet.
        top_names = {p.name for p in final_dir.iterdir() if not p.name.startswith(".")}
        observed["top_names_during_backend"] = top_names
        observed["staging_hidden"] = staging.name.startswith(".")
        observed["staging_inside_final"] = staging.parent == final_dir

    _install_stubs(monkeypatch, fake_backend)
    media = _make_media(tmp_path)
    out_dir = tmp_path / "out"

    api.transcribe(url_or_file=str(media), output_dir=str(out_dir), model="tiny", device="cpu")

    assert observed["top_names_during_backend"] == set()
    assert observed["staging_hidden"] is True
    assert observed["staging_inside_final"] is True


def test_backend_failure_leaves_previous_round_intact(tmp_path: Path, monkeypatch) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    for name in CORE:
        (out_dir / name).write_bytes(f"old:{name}".encode())

    def fake_backend(input_wav, device, model, output_dir, task, language, other_args) -> None:
        d = Path(output_dir)
        (d / "clip.wav.txt").write_bytes(b"new:txt")
        # no srt -> validation must reject the round

    _install_stubs(monkeypatch, fake_backend)
    media = _make_media(tmp_path)

    with pytest.raises(Exception):  # noqa: B017 - validation error propagates
        api.transcribe(url_or_file=str(media), output_dir=str(out_dir), model="tiny", device="cpu")

    assert list_managed_artifacts(out_dir) == sorted(CORE)
    for name in CORE:
        assert (out_dir / name).read_bytes() == f"old:{name}".encode()
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())


def test_embed_success_publishes_burned_mp4_from_staging(tmp_path: Path, monkeypatch) -> None:
    def fake_backend(input_wav, device, model, output_dir, task, language, other_args) -> None:
        _write_core(Path(output_dir), "new")

    burned: list[Path] = []

    def fake_embed(media_file, srt_file, out_mp4) -> None:
        Path(out_mp4).write_bytes(b"burned-mp4")
        burned.append(Path(out_mp4))

    _install_stubs(monkeypatch, fake_backend)
    monkeypatch.setattr(api, "_embed_subtitles", fake_embed)
    media = _make_media(tmp_path)
    out_dir = tmp_path / "out"

    api.transcribe(url_or_file=str(media), output_dir=str(out_dir), model="tiny", device="cpu", embed=True)

    assert burned  # ffmpeg burn was invoked into the staging directory
    assert burned[0].parent.name.startswith(".staging-")
    assert (out_dir / "out.mp4").read_bytes() == b"burned-mp4"
    assert set(list_managed_artifacts(out_dir)) == {*CORE, "out.mp4"}


def test_embed_failure_keeps_old_mp4_intact(tmp_path: Path, monkeypatch) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    for name in CORE:
        (out_dir / name).write_bytes(f"old:{name}".encode())
    (out_dir / "out.mp4").write_bytes(b"old-mp4")

    def fake_backend(input_wav, device, model, output_dir, task, language, other_args) -> None:
        _write_core(Path(output_dir), "new")

    def failing_embed(media_file, srt_file, out_mp4) -> None:
        raise subprocess.CalledProcessError(1, ["ffmpeg"], output=b"bad", stderr=b"boom")

    _install_stubs(monkeypatch, fake_backend)
    monkeypatch.setattr(api, "_embed_subtitles", failing_embed)
    media = _make_media(tmp_path)

    with pytest.raises(subprocess.CalledProcessError):
        api.transcribe(url_or_file=str(media), output_dir=str(out_dir), model="tiny", device="cpu", embed=True)

    assert (out_dir / "out.mp4").read_bytes() == b"old-mp4"
    for name in CORE:
        assert (out_dir / name).read_bytes() == f"old:{name}".encode()
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())


def test_second_round_without_optional_files_removes_them(tmp_path: Path, monkeypatch) -> None:
    media = _make_media(tmp_path)
    out_dir = tmp_path / "out"

    def backend_with_extras(input_wav, device, model, output_dir, task, language, other_args) -> None:
        d = Path(output_dir)
        _write_core(d, "r1")
        (d / "speaker.json").write_bytes(b'{"speakers": []}')

    def fake_embed(media_file, srt_file, out_mp4) -> None:
        Path(out_mp4).write_bytes(b"r1-mp4")

    _install_stubs(monkeypatch, backend_with_extras)
    monkeypatch.setattr(api, "_embed_subtitles", fake_embed)
    api.transcribe(url_or_file=str(media), output_dir=str(out_dir), model="tiny", device="cpu", embed=True)
    assert set(list_managed_artifacts(out_dir)) == {*CORE, "speaker.json", "out.mp4"}

    def backend_core_only(input_wav, device, model, output_dir, task, language, other_args) -> None:
        _write_core(Path(output_dir), "r2")

    _install_stubs(monkeypatch, backend_core_only)
    api.transcribe(url_or_file=str(media), output_dir=str(out_dir), model="tiny", device="cpu")

    assert set(list_managed_artifacts(out_dir)) == set(CORE)
    assert (out_dir / "out.srt").read_bytes() == b"r2:srt"
    assert not any(p.name.startswith(".") for p in out_dir.iterdir())
