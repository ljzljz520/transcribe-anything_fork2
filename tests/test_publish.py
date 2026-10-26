"""
Tests for the atomic round publishing protocol (``transcribe_anything.publish``).

Covers: name classification, per-directory locks (threads + cross-process
lock directory + stale lock reaping), staging validation, atomic commit,
stale artifact removal, user-file preservation, crash recovery at every
phase, and concurrent writes.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from transcribe_anything.publish import (
    JOURNAL_NAME,
    LOCK_DIRNAME,
    STAGING_PREFIX,
    ArtifactValidationError,
    RoundLockTimeout,
    commit_round,
    is_internal_name,
    is_managed_artifact,
    list_managed_artifacts,
    list_staging_dirs,
    recover_rounds,
    staged_round,
)

CORE = ["out.txt", "out.srt", "out.vtt", "out.json"]


def _write_round(stage_dir: Path, names: list[str], tag: str) -> None:
    for name in names:
        (stage_dir / name).write_bytes(f"{tag}:{name}".encode())


def _seed_old_round(out: Path, names: list[str], tag: str = "old") -> None:
    out.mkdir(parents=True, exist_ok=True)
    _write_round(out, names, tag)


def _hash_dir(out: Path) -> dict[str, bytes]:
    """Content hash of user-visible entries only (dot-internals excluded)."""
    snap = {}
    for p in sorted(out.rglob("*")):
        if p.is_file():
            rel = p.relative_to(out)
            if rel.parts[0].startswith("."):
                continue
            snap[str(rel)] = p.read_bytes()
    return snap


def _no_internal_entries(out: Path) -> bool:
    return not any(p.name.startswith(".") for p in out.iterdir())


# ---------------------------------------------------------------------------
# TR-1.4: name classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,managed,internal",
    [
        ("out.txt", True, False),
        ("out.srt", True, False),
        ("out.vtt", True, False),
        ("out.json", True, False),
        ("out.tsv", True, False),
        ("speaker.json", True, False),
        ("out.mp4", True, False),
        ("notes.txt", False, False),
        ("_input", False, False),
        (".staging-abc", False, True),
        (JOURNAL_NAME, False, True),
        (LOCK_DIRNAME, False, True),
    ],
)
def test_name_classification(name: str, managed: bool, internal: bool) -> None:
    assert is_managed_artifact(name) is managed
    assert is_internal_name(name) is internal


# ---------------------------------------------------------------------------
# TR-1.1: in-process locks
# ---------------------------------------------------------------------------


def test_same_dir_lock_serializes_threads(tmp_path: Path) -> None:
    order: list[str] = []

    def worker(label: str) -> None:
        with staged_round(tmp_path, timeout=10) as stage:
            order.append(f"enter-{label}")
            time.sleep(0.1)
            _write_round(stage.path, CORE, label)
            commit_round(stage)
            order.append(f"exit-{label}")

    t1 = threading.Thread(target=worker, args=("a",))
    t2 = threading.Thread(target=worker, args=("b",))
    t1.start()
    time.sleep(0.02)
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)
    # B never enters before A exits (whole-round serialization).
    assert order.index("exit-a") < order.index("enter-b")


def test_distinct_dirs_do_not_block_each_other(tmp_path: Path) -> None:
    barrier = threading.Barrier(2, timeout=10)
    results: list[str] = []

    def worker(label: str, out: Path) -> None:
        with staged_round(out, timeout=10) as stage:
            barrier.wait()
            results.append(f"in-{label}")
            _write_round(stage.path, CORE, label)
            commit_round(stage)

    threads = [
        threading.Thread(target=worker, args=("a", tmp_path / "a")),
        threading.Thread(target=worker, args=("b", tmp_path / "b")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert set(results) == {"in-a", "in-b"}


def test_lock_timeout_raises(tmp_path: Path) -> None:
    holder: list[None] = []

    def hold() -> None:
        with staged_round(tmp_path, timeout=5):
            holder.append(None)
            time.sleep(0.6)

    t = threading.Thread(target=hold)
    t.start()
    time.sleep(0.1)
    with pytest.raises(RoundLockTimeout), staged_round(tmp_path, timeout=0.1):
        pass
    t.join(timeout=5)


# ---------------------------------------------------------------------------
# TR-1.2: stale lock reaping (dead owner pid)
# ---------------------------------------------------------------------------


def test_stale_lock_dir_with_dead_pid_is_reaped(tmp_path: Path) -> None:
    lock_dir = tmp_path / LOCK_DIRNAME
    lock_dir.mkdir()
    (lock_dir / "owner.json").write_text(json.dumps({"host": os.uname().nodename if hasattr(os, "uname") else "localhost", "pid": 999999, "created": time.time()}))
    # A dead owner must not block a fresh round.
    with staged_round(tmp_path, timeout=5) as stage:
        _write_round(stage.path, CORE, "new")
        commit_round(stage)
    assert list_managed_artifacts(tmp_path) == sorted(CORE)


# ---------------------------------------------------------------------------
# TR-1.3: sweep of orphan staging dirs
# ---------------------------------------------------------------------------


def test_recover_sweeps_orphan_staging_and_keeps_round(tmp_path: Path) -> None:
    _seed_old_round(tmp_path, CORE + ["out.mp4"])
    (tmp_path / "notes.txt").write_text("mine", encoding="utf-8")
    orphan = tmp_path / f"{STAGING_PREFIX}deadbeef"
    orphan.mkdir()
    (orphan / "out.srt").write_bytes(b"partial")
    before = _hash_dir(tmp_path)

    recover_rounds(tmp_path)

    assert list_staging_dirs(tmp_path) == []
    assert _hash_dir(tmp_path) == before  # old round + user file untouched


# ---------------------------------------------------------------------------
# TR-2.1: validation gates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "staged_files,expected_fragment",
    [
        ({"out.txt": b"x", "out.vtt": b"x", "out.json": b"{}"}, "missing required"),
        ({"out.txt": b"x", "out.srt": b"", "out.vtt": b"x", "out.json": b"{}"}, "empty"),
        ({"out.txt": b"x", "out.srt": b"x", "out.vtt": b"x", "out.json": b"{}", "stray.md": b"x"}, "unexpected"),
    ],
)
def test_validation_failures_leave_old_round_intact(tmp_path: Path, staged_files: dict, expected_fragment: str) -> None:
    _seed_old_round(tmp_path, CORE)
    before = _hash_dir(tmp_path)
    with pytest.raises(ArtifactValidationError) as exc, staged_round(tmp_path) as stage:
        for name, data in staged_files.items():
            (stage.path / name).write_bytes(data)
        commit_round(stage)
    assert expected_fragment in str(exc.value)
    assert _hash_dir(tmp_path) == before
    assert _no_internal_entries(tmp_path)


# ---------------------------------------------------------------------------
# TR-2.2 / TR-2.3 / AC-6: publish exact set, remove stale, keep user files
# ---------------------------------------------------------------------------


def test_commit_replaces_removes_stale_and_preserves_user_entries(tmp_path: Path) -> None:
    # Previous round carried speaker.json and out.mp4; new round drops both.
    _seed_old_round(tmp_path, CORE + ["speaker.json", "out.mp4"], tag="old")
    (tmp_path / "notes.txt").write_text("user notes", encoding="utf-8")
    (tmp_path / "_input").mkdir()
    (tmp_path / "_input" / "upload.bin").write_bytes(b"uploaded")

    with staged_round(tmp_path) as stage:
        _write_round(stage.path, CORE, "new")
        commit_round(stage)

    assert list_managed_artifacts(tmp_path) == sorted(CORE)
    assert not (tmp_path / "speaker.json").exists()
    assert not (tmp_path / "out.mp4").exists()
    for name in CORE:
        assert (tmp_path / name).read_bytes() == f"new:{name}".encode()
    assert (tmp_path / "notes.txt").read_text(encoding="utf-8") == "user notes"
    assert (tmp_path / "_input" / "upload.bin").read_bytes() == b"uploaded"
    assert _no_internal_entries(tmp_path)


def test_commit_overwrites_existing_managed_files(tmp_path: Path) -> None:
    _seed_old_round(tmp_path, CORE, tag="old")
    with staged_round(tmp_path) as stage:
        _write_round(stage.path, CORE + ["speaker.json"], "new")
        commit_round(stage)
    assert set(list_managed_artifacts(tmp_path)) == set(CORE + ["speaker.json"])
    assert (tmp_path / "out.srt").read_bytes() == b"new:out.srt"


def test_openai_whisper_tsv_round_then_core_round_removes_tsv(tmp_path: Path) -> None:
    # openai-whisper's default formats include tsv; the next backend/round
    # that doesn't emit it must see out.tsv cleaned up.
    with staged_round(tmp_path) as stage:
        _write_round(stage.path, CORE + ["out.tsv"], "w")
        commit_round(stage)
    assert set(list_managed_artifacts(tmp_path)) == set(CORE + ["out.tsv"])

    with staged_round(tmp_path) as stage:
        _write_round(stage.path, CORE, "w2")
        commit_round(stage)
    assert set(list_managed_artifacts(tmp_path)) == set(CORE)
    assert not (tmp_path / "out.tsv").exists()


# ---------------------------------------------------------------------------
# TR-2.4 / AC-7: crash recovery at every phase
# ---------------------------------------------------------------------------


def _write_pending_journal(out: Path, round_id: str, new: list[str], stale: list[str]) -> Path:
    staging = out / f"{STAGING_PREFIX}{round_id}"
    staging.mkdir(parents=True, exist_ok=True)
    journal = {"version": 1, "round_id": round_id, "new": new, "stale": stale}
    (out / JOURNAL_NAME).write_text(json.dumps(journal), encoding="utf-8")
    return staging


def test_recovers_pre_commit_crash_to_old_round(tmp_path: Path) -> None:
    _seed_old_round(tmp_path, CORE + ["out.mp4"], tag="old")
    orphan = tmp_path / f"{STAGING_PREFIX}aaaa"
    orphan.mkdir()
    (orphan / "out.txt").write_bytes(b"half-new")

    with staged_round(tmp_path) as stage:  # entry triggers recovery
        _write_round(stage.path, CORE, "new")
        commit_round(stage)

    assert list_managed_artifacts(tmp_path) == sorted(CORE)  # old mp4 stale per new set
    assert (tmp_path / "out.txt").read_bytes() == b"new:out.txt"
    assert _no_internal_entries(tmp_path)


def test_recovers_mid_install_rolls_forward_to_new_round(tmp_path: Path) -> None:
    _seed_old_round(tmp_path, CORE + ["speaker.json", "out.mp4"], tag="old")
    round_id = "cccc1111"
    staging = _write_pending_journal(tmp_path, round_id, CORE, ["speaker.json", "out.mp4"])
    _write_round(staging, CORE, "new")
    # Simulate crash after out.txt/out.srt were already replaced:
    os.replace(staging / "out.txt", tmp_path / "out.txt")
    os.replace(staging / "out.srt", tmp_path / "out.srt")
    # ...new vtt/json still only in staging, stale files still in place.

    recover_rounds(tmp_path)

    assert set(list_managed_artifacts(tmp_path)) == set(CORE)
    for name in CORE:
        assert (tmp_path / name).read_bytes() == f"new:{name}".encode()
    assert not (tmp_path / "speaker.json").exists()
    assert not (tmp_path / "out.mp4").exists()
    assert _no_internal_entries(tmp_path)


def test_recovers_after_replaces_before_stale_delete(tmp_path: Path) -> None:
    _seed_old_round(tmp_path, CORE + ["out.mp4"], tag="old")
    round_id = "dddd2222"
    staging = _write_pending_journal(tmp_path, round_id, CORE, ["out.mp4"])
    _write_round(staging, CORE, "new")
    for name in CORE:  # all new files installed...
        os.replace(staging / name, tmp_path / name)
    # ...but stale out.mp4 removal and journal cleanup never happened.

    recover_rounds(tmp_path)

    assert set(list_managed_artifacts(tmp_path)) == set(CORE)
    assert not (tmp_path / "out.mp4").exists()
    assert _no_internal_entries(tmp_path)


def test_recovers_journal_without_staging_closes_round(tmp_path: Path) -> None:
    # Staging is removed only after the install completes, so this state
    # means the new round is fully on disk; just drop the journal.
    _seed_old_round(tmp_path, CORE, tag="new")
    staging = _write_pending_journal(tmp_path, "eeee3333", CORE, [])
    shutil.rmtree(staging)
    before = _hash_dir(tmp_path)

    recover_rounds(tmp_path)

    assert _hash_dir(tmp_path) == before
    assert _no_internal_entries(tmp_path)


# ---------------------------------------------------------------------------
# Same-thread (reentrant) nesting: an inner round must not sweep an outer
# live staging. Models two asyncio tasks on one thread hitting one dir.
# ---------------------------------------------------------------------------


def test_nested_same_thread_round_preserves_outer_staging(tmp_path: Path) -> None:
    _seed_old_round(tmp_path, CORE, tag="old")
    # A pre-commit orphan that predates the outer round: only the outer
    # (non-reentered) entry is allowed to sweep it.
    orphan = tmp_path / f"{STAGING_PREFIX}orphan"
    orphan.mkdir(parents=True)
    (orphan / "out.srt").write_bytes(b"leftover")

    with staged_round(tmp_path) as outer:
        assert not orphan.exists()  # outer entry reconciled the directory
        _write_round(outer.path, CORE, "outer")

        # An explicit nested recover is a no-op and must not sweep outer.
        recover_rounds(tmp_path)
        assert outer.path.is_dir()

        with staged_round(tmp_path) as inner:
            assert inner.path != outer.path
            _write_round(inner.path, CORE, "inner")
            # Inner reentrant entry skipped recovery: outer staging alive.
            assert outer.path.is_dir()
        # Inner abandoned without committing -> only its own staging swept.
        assert not inner.path.exists()
        assert outer.path.is_dir()
        assert (outer.path / "out.srt").read_bytes() == b"outer:out.srt"

        commit_round(outer)

    assert set(list_managed_artifacts(tmp_path)) == set(CORE)
    assert (tmp_path / "out.srt").read_bytes() == b"outer:out.srt"
    assert _no_internal_entries(tmp_path)


# ---------------------------------------------------------------------------
# TR-6.1: concurrent writes into one directory
# ---------------------------------------------------------------------------


def test_concurrent_rounds_same_directory(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("mine", encoding="utf-8")
    variants = {
        0: CORE,
        1: CORE + ["speaker.json"],
        2: CORE + ["speaker.json", "out.mp4"],
        3: CORE + ["out.mp4"],
    }
    errors: list[BaseException] = []

    def worker(worker_id: int) -> None:
        try:
            for rnd in range(5):
                variant = (worker_id * 5 + rnd) % 4
                tag = f"w{worker_id}r{rnd}v{variant}"
                with staged_round(tmp_path, timeout=30) as stage:
                    _write_round(stage.path, variants[variant], tag)
                    commit_round(stage)
        except Exception as exc:  # noqa: BLE001 - harness records any worker failure
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    assert not errors, errors

    names = list_managed_artifacts(tmp_path)
    tags = {(tmp_path / n).read_bytes().split(b":", 1)[0].decode() for n in names}
    assert len(tags) == 1, f"mixed rounds on disk: {tags}"
    tag = tags.pop()
    variant = int(tag.rsplit("v", 1)[1])
    assert set(names) == set(variants[variant])
    assert (tmp_path / "notes.txt").read_text(encoding="utf-8") == "mine"
    assert _no_internal_entries(tmp_path)


# ---------------------------------------------------------------------------
# TR-6.2: concurrent writes into distinct directories
# ---------------------------------------------------------------------------


def test_concurrent_rounds_distinct_directories(tmp_path: Path) -> None:
    dirs = [tmp_path / f"d{i}" for i in range(4)]

    def worker(out: Path, label: str) -> None:
        for rnd in range(5):
            with staged_round(out, timeout=30) as stage:
                _write_round(stage.path, CORE, f"{label}{rnd}")
                commit_round(stage)

    threads = [threading.Thread(target=worker, args=(d, f"d{i}")) for i, d in enumerate(dirs)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    for d in dirs:
        assert set(list_managed_artifacts(d)) == set(CORE)
        assert _no_internal_entries(d)


# ---------------------------------------------------------------------------
# TR-6.3: real cross-process lock: blocks a live owner, reaps a killed one
# ---------------------------------------------------------------------------


def test_cross_process_lock_blocks_then_reaps(tmp_path: Path) -> None:
    src_dir = Path(__file__).resolve().parents[1] / "src"
    script = textwrap.dedent(f"""
        import sys, time
        sys.path.insert(0, {str(src_dir)!r})
        from transcribe_anything.publish import staged_round
        with staged_round({str(tmp_path)!r}, timeout=5) as stage:
            (stage.path / "hold").write_text("x", encoding="utf-8")
            time.sleep(4)
        """)
    env = dict(os.environ)
    child = subprocess.Popen([sys.executable, "-c", script], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        deadline = time.time() + 10
        while not (tmp_path / LOCK_DIRNAME).exists() and time.time() < deadline:
            time.sleep(0.05)
        assert (tmp_path / LOCK_DIRNAME).exists()
        # Live owner -> the parent cannot acquire.
        with pytest.raises(RoundLockTimeout), staged_round(tmp_path, timeout=0.5):
            pass
    finally:
        child.kill()
        child.communicate(timeout=10)

    # Killed owner -> stale lock is reclaimed; orphan staging is swept on
    # entry, and the new round publishes normally.
    with staged_round(tmp_path, timeout=10) as stage:
        _write_round(stage.path, CORE, "parent")
        commit_round(stage)
    assert set(list_managed_artifacts(tmp_path)) == set(CORE)
    assert _no_internal_entries(tmp_path)
    assert child.returncode != 0  # child was killed as designed
