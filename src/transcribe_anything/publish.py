"""
Atomic, crash-safe publishing of one transcription round.

A "round" is the complete artifact set produced by one ``transcribe()``
call or one remote job download:

    out.txt / out.srt / out.vtt / out.json / out.tsv
    optional: speaker.json, out.mp4 (burned subtitles)

The publish protocol guarantees that a consumer observing the final
``output_dir`` can only ever see a *complete* round — either the previous
one or the new one, never a partial mix:

1. All products of the new round are written into a hidden staging
   directory (``.staging-<round_id>/``) inside the final directory, so
   every later rename stays on one filesystem and is therefore atomic.
2. The staged set is validated (required files present, every managed
   artifact a non-empty regular file).
3. A journal listing the new files and the stale files to remove is
   atomically installed at ``.transcribe-round.json`` *before* the final
   directory is touched.
4. New files are ``os.replace()``d into place (atomic per file), then
   stale managed files are deleted. The journal is removed last.

Crash recovery (``recover_rounds`` / automatic on ``staged_round`` entry):

* journal present + staging alive -> roll the install forward to the
  complete new round;
* journal present + staging gone  -> the install had already finished
  (staging is only removed after all replaces/deletes); drop the journal;
* no journal + leftover staging   -> pre-commit crash; sweep the staging
  directory, the previous complete round is untouched.

Concurrency is serialized per resolved output directory by an in-process
reentrant lock plus a cross-process lock directory (``mkdir`` is atomic)
whose owner is recorded; locks left by dead processes are reclaimed.

The module is dependency-free (stdlib only) so it can be imported from
the FastAPI-free core, the local API, and the HTTP client alike.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import socket
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Managed names
# ---------------------------------------------------------------------------

#: File names a round is allowed to publish.
MANAGED_ARTIFACTS = frozenset(
    {
        "out.txt",
        "out.srt",
        "out.vtt",
        "out.json",
        "out.tsv",
        "speaker.json",
        "out.mp4",
    }
)

#: Artifacts that must exist (and be non-empty) before publishing.
REQUIRED_ARTIFACTS = frozenset({"out.srt"})

STAGING_PREFIX = ".staging-"
JOURNAL_NAME = ".transcribe-round.json"
LOCK_DIRNAME = ".transcribe-round.lock"
_LOCK_OWNER_NAME = "owner.json"

#: Poll interval while waiting on the cross-process lock.
LOCK_POLL_SECONDS = 0.05

#: A lock directory whose owner file is missing/unreadable is reclaimed
#: only after it reaches this age (guards the tiny window between the
#: atomic ``mkdir`` and the owner file being written).
STALE_LOCK_AGE_SECONDS = 60.0

JOURNAL_VERSION = 1


class ArtifactValidationError(Exception):
    """Raised when a staged round is incomplete or corrupt."""


class RoundLockTimeout(TimeoutError):
    """Raised when the per-directory round lock cannot be acquired in time."""


def is_managed_artifact(name: str) -> bool:
    """True for artifact names owned by the round mechanism."""
    return name in MANAGED_ARTIFACTS


def is_internal_name(name: str) -> bool:
    """True for dot-prefixed bookkeeping entries never exposed to users."""
    return name.startswith(".")


def list_managed_artifacts(output_dir: os.PathLike | str) -> list[str]:
    """Sorted managed artifact file names currently present in ``output_dir``."""
    path = Path(output_dir)
    if not path.is_dir():
        return []
    return sorted(p.name for p in path.iterdir() if p.is_file() and is_managed_artifact(p.name))


def list_staging_dirs(output_dir: os.PathLike | str) -> list[Path]:
    """All leftover staging directories in ``output_dir``."""
    path = Path(output_dir)
    if not path.is_dir():
        return []
    return sorted(p for p in path.iterdir() if p.is_dir() and p.name.startswith(STAGING_PREFIX))


# ---------------------------------------------------------------------------
# fsync helpers (best effort — some platforms/filesystems reject fsync)
# ---------------------------------------------------------------------------


def _fsync_file(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY
    dir_fd = getattr(os, "O_DIRECTORY", 0)
    if dir_fd:
        flags |= dir_fd
    try:
        fd = os.open(str(path), flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# Per-directory locks (in-process reentrant + cross-process lock directory)
# ---------------------------------------------------------------------------

_registry_guard = threading.Lock()
_thread_locks: dict[str, threading.RLock] = {}
#: Reentrancy depth for cross-process locks already held by THIS process.
_process_locks_held: dict[str, int] = {}


def _pid_alive(pid: int) -> bool:
    """Best-effort liveness check for ``pid`` on the local host."""
    if pid <= 0:
        return False
    if sys.platform == "win32":  # pragma: no cover - exercised on Windows only
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        ERROR_INVALID_PARAMETER = 87
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            # Cannot tell -> assume alive, except an explicit "no such pid".
            return ctypes.get_last_error() != ERROR_INVALID_PARAMETER  # type: ignore[union-attr]
        kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def _read_lock_owner(lock_dir: Path) -> tuple[str, int] | None:
    owner_file = lock_dir / _LOCK_OWNER_NAME
    try:
        data = json.loads(owner_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    try:
        return str(data["host"]), int(data["pid"])
    except (KeyError, TypeError, ValueError):
        return None


def _lock_dir_is_stale(lock_dir: Path) -> bool:
    """A lock dir is stale if its owner is dead, or it is old and ownerless."""
    owner = _read_lock_owner(lock_dir)
    if owner is None:
        try:
            age = time.time() - lock_dir.stat().st_mtime
        except OSError:
            return False
        return age > STALE_LOCK_AGE_SECONDS
    host, pid = owner
    if host != socket.gethostname():
        # Remote host (shared drive) — nothing safe we can do.
        return False
    return not _pid_alive(pid)


def _reap_lock_dir(lock_dir: Path) -> None:
    """Atomically claim a stale lock directory, then remove it.

    The rename ensures that among several reclaimers exactly one wins.
    """
    tombstone = lock_dir.with_name(f"{LOCK_DIRNAME}.reaped-{uuid.uuid4().hex}")
    try:
        os.rename(lock_dir, tombstone)
    except FileNotFoundError:
        return
    shutil.rmtree(tombstone, ignore_errors=True)


def _acquire_lock_dir(output_dir: Path, timeout: float) -> None:
    lock_dir = output_dir / LOCK_DIRNAME
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        try:
            lock_dir.mkdir()
        except FileExistsError:
            if _lock_dir_is_stale(lock_dir):
                _reap_lock_dir(lock_dir)
                continue
            if time.monotonic() >= deadline:
                raise RoundLockTimeout(f"another round is active in {output_dir}")
            time.sleep(LOCK_POLL_SECONDS)
            continue
        # We own the directory now — record the owner before anyone inspects it.
        owner_file = lock_dir / _LOCK_OWNER_NAME
        owner = {"host": socket.gethostname(), "pid": os.getpid(), "created": time.time()}
        owner_file.write_text(json.dumps(owner), encoding="utf-8")
        _fsync_file(owner_file)
        _fsync_dir(lock_dir)
        return


class _RoundGuard:
    """Exclusive access to one output directory for the duration of a round."""

    def __init__(self, key: str, output_dir: Path, proc_reentered: bool) -> None:
        self.key = key
        self.output_dir = output_dir
        self._proc_reentered = proc_reentered
        self._released = False

    @property
    def reentered(self) -> bool:
        """True when an outer holder on this same thread already owns the lock.

        The RLock is thread-bound, so a reentrant acquire is always the same
        thread nesting inside a live round. Only the outer acquisition runs
        crash recovery: the outer entry reconciled the directory before
        creating its staging, and every staging created afterwards belongs to
        a live same-process (same-thread) holder and must not be swept.
        """
        return self._proc_reentered

    @classmethod
    def acquire(cls, output_dir: Path, timeout: float) -> _RoundGuard:
        key = str(output_dir)
        with _registry_guard:
            thread_lock = _thread_locks.setdefault(key, threading.RLock())
        if not thread_lock.acquire(timeout=max(0.0, timeout)):
            raise RoundLockTimeout(f"timed out waiting for round lock on {output_dir}")
        try:
            with _registry_guard:
                depth = _process_locks_held.get(key, 0)
                if depth > 0:
                    # Same process/thread already owns the cross-process lock
                    # (the per-key RLock keeps every other thread out).
                    _process_locks_held[key] = depth + 1
                    proc_reentered = True
                else:
                    _acquire_lock_dir(output_dir, timeout)
                    _process_locks_held[key] = 1
                    proc_reentered = False
        except BaseException:
            thread_lock.release()
            raise
        return cls(key, output_dir, proc_reentered)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            with _registry_guard:
                depth = _process_locks_held.get(self.key, 0)
                if self._proc_reentered:
                    if depth > 1:
                        _process_locks_held[self.key] = depth - 1
                else:
                    if depth <= 1:
                        _process_locks_held.pop(self.key, None)
                        shutil.rmtree(self.output_dir / LOCK_DIRNAME, ignore_errors=True)
                    else:
                        _process_locks_held[self.key] = depth - 1
        finally:
            with _registry_guard:
                thread_lock = _thread_locks.get(self.key)
            if thread_lock is not None:
                thread_lock.release()


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


def _read_journal(output_dir: Path) -> dict | None:
    journal_path = output_dir / JOURNAL_NAME
    try:
        raw = journal_path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(data, dict) or not isinstance(data.get("round_id"), str):
        return None
    return data


def _roll_forward(output_dir: Path, journal: dict, staging: Path | None) -> None:
    """Finish an install that was interrupted after the journal went down."""
    new_names = journal.get("new", [])
    stale_names = journal.get("stale", [])
    if isinstance(new_names, str) or not isinstance(new_names, list):
        new_names = []
    if isinstance(stale_names, str) or not isinstance(stale_names, list):
        stale_names = []
    if staging is not None and staging.is_dir():
        for name in new_names:
            if not isinstance(name, str) or not is_managed_artifact(name):
                continue
            src = staging / name
            dst = output_dir / name
            if src.is_file():
                os.replace(src, dst)
        for name in stale_names:
            if not isinstance(name, str) or not is_managed_artifact(name):
                continue
            try:
                (output_dir / name).unlink()
            except FileNotFoundError:
                pass
        _fsync_dir(output_dir)
        shutil.rmtree(staging, ignore_errors=True)
    # Staging gone => the install had completed (staging is removed only
    # after all replaces and stale deletes); just close the journal.
    try:
        (output_dir / JOURNAL_NAME).unlink()
    except FileNotFoundError:
        pass
    _fsync_dir(output_dir)


def _recover_locked(output_dir: Path) -> None:
    """Recovery internals. Caller must hold the round guard."""
    journal = _read_journal(output_dir)
    leftover = list_staging_dirs(output_dir)
    if journal is not None:
        staging = output_dir / f"{STAGING_PREFIX}{journal['round_id']}"
        _roll_forward(output_dir, journal, staging)
        leftover = [p for p in leftover if p != staging]
    # Pre-commit crashes leave staging behind without a journal: sweep them.
    # A live round cannot be swept because it holds the round guard.
    for staging in leftover:
        shutil.rmtree(staging, ignore_errors=True)


def recover_rounds(output_dir: os.PathLike | str, *, timeout: float = 30.0) -> None:
    """Reconcile ``output_dir`` after a previous interrupted round.

    Safe to call at any time; no-op when the directory is already in a
    clean state.
    """
    out = Path(output_dir).resolve()
    if not out.is_dir():
        return
    guard = _RoundGuard.acquire(out, timeout)
    try:
        # A nested same-thread caller already lives inside a reconciled
        # round; recovering again would sweep the outer live staging.
        if not guard.reentered:
            _recover_locked(out)
    finally:
        guard.release()


# ---------------------------------------------------------------------------
# Staging, validation, commit
# ---------------------------------------------------------------------------


@dataclass
class StagedRound:
    """Handle to one round's staging area."""

    output_dir: Path
    path: Path
    round_id: str
    committed: bool = False
    _guard: _RoundGuard | None = field(default=None, repr=False)


@contextlib.contextmanager
def staged_round(
    output_dir: os.PathLike | str,
    *,
    timeout: float = 30.0,
) -> Iterator[StagedRound]:
    """Context for building one round in isolation, then publishing it.

    On entry: the final directory is created, any interrupted previous
    round is recovered, and a fresh hidden staging directory is created
    inside it. The per-directory round lock is held until exit, so
    concurrent rounds on the same directory are serialized end to end.

    On exit without :func:`commit_round` the staging directory is removed
    and the previous complete round is left untouched. If a commit was
    interrupted after its journal went down, staging is preserved for the
    next entry's roll-forward (the journal is the authority).
    """
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    guard = _RoundGuard.acquire(out, timeout)
    staging: Path | None = None
    stage: StagedRound | None = None
    try:
        # Recovery runs only for the outer holder. A nested same-thread
        # entry (e.g. two asyncio downloads to one directory sharing one
        # thread) must not sweep the outer holder's live staging; the outer
        # entry already reconciled everything before it staged anything.
        if not guard.reentered:
            _recover_locked(out)
        round_id = uuid.uuid4().hex
        staging = out / f"{STAGING_PREFIX}{round_id}"
        staging.mkdir()
        stage = StagedRound(output_dir=out, path=staging, round_id=round_id, _guard=guard)
        yield stage
    except BaseException:
        # Preserve an interrupted commit's staging/journal pair; sweep
        # anything else (validation/backend failures happened pre-journal).
        if stage is not None and not stage.committed and not (out / JOURNAL_NAME).is_file():
            shutil.rmtree(stage.path, ignore_errors=True)
        raise
    else:
        if stage is not None and not stage.committed:
            shutil.rmtree(stage.path, ignore_errors=True)
    finally:
        guard.release()


def validate_staging(stage: StagedRound) -> list[str]:
    """Validate the staged set. Returns the sorted new-file name list.

    Raises :class:`ArtifactValidationError` on the first problem:
    unknown/non-regular entry, empty file, or a missing required artifact.
    """
    names: list[str] = []
    for entry in sorted(stage.path.iterdir(), key=lambda p: p.name):
        if entry.name == JOURNAL_NAME:
            continue
        if not entry.is_file() or not is_managed_artifact(entry.name):
            raise ArtifactValidationError(f"unexpected staging entry: {entry.name!r}")
        if entry.stat().st_size == 0:
            raise ArtifactValidationError(f"artifact is empty: {entry.name}")
        names.append(entry.name)
    missing = sorted(REQUIRED_ARTIFACTS.difference(names))
    if missing:
        raise ArtifactValidationError(f"missing required artifacts: {', '.join(missing)}")
    return sorted(names)


def _write_pending_journal(stage: StagedRound, new_names: list[str], stale_names: list[str]) -> None:
    journal = {
        "version": JOURNAL_VERSION,
        "round_id": stage.round_id,
        "new": new_names,
        "stale": stale_names,
    }
    # Journal is first materialized inside staging (same filesystem), then
    # atomically moved into the final directory — no half-written journal
    # ever appears at the journal path.
    tmp_journal = stage.path / JOURNAL_NAME
    tmp_journal.write_text(json.dumps(journal, indent=2, sort_keys=True), encoding="utf-8")
    _fsync_file(tmp_journal)
    _fsync_dir(stage.path)
    os.replace(tmp_journal, stage.output_dir / JOURNAL_NAME)
    _fsync_dir(stage.output_dir)


def commit_round(stage: StagedRound) -> None:
    """Validate and atomically publish the staged round.

    Must be called inside the ``staged_round`` context. After this
    returns, the final directory contains exactly the new managed set,
    stale managed files are gone, user files are untouched, and the
    staging directory and journal are removed.
    """
    if stage.committed:
        raise ArtifactValidationError("round already committed")
    if stage._guard is None:  # defensive; normal callers use the context
        raise ArtifactValidationError("commit_round requires a staged_round() context")
    out = stage.output_dir
    new_names = validate_staging(stage)
    stale_names = sorted(set(list_managed_artifacts(out)).difference(new_names))

    # From this point on an interruption is recoverable via the journal.
    _write_pending_journal(stage, new_names, stale_names)

    for name in new_names:
        src = stage.path / name
        if src.is_file():
            os.replace(src, out / name)
    for name in stale_names:
        try:
            (out / name).unlink()
        except FileNotFoundError:
            pass
    _fsync_dir(out)

    # Install is complete; staging only holds nothing now. Removing the
    # journal last makes "journal absent" the committed marker.
    shutil.rmtree(stage.path, ignore_errors=True)
    try:
        (out / JOURNAL_NAME).unlink()
    except FileNotFoundError:
        pass
    _fsync_dir(out)
    stage.committed = True
