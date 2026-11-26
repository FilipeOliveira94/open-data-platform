"""``run_python_args`` stops the child's whole process group (P3-04).

The child script starts a grandchild, writes both pids to a file, and
sleeps. Each test checks that neither process is alive afterwards. A
SIGALRM handler that raises stands in for the Airflow task timeout, which
also arrives as an exception while the task waits.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
DAG_DIR = ROOT / "airflow" / "dags"

pytestmark = pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX")
# ingest_common imports Airflow, the uv `airflow` group (P3-10).
pytest.importorskip("airflow", reason="uv group 'airflow' is not installed")

CHILD = """
import os, signal, subprocess, sys, time
from pathlib import Path
grandchild_code = sys.argv[3]
grandchild = subprocess.Popen([sys.executable, "-c", grandchild_code])
Path(sys.argv[1]).write_text(f"{os.getpid()} {grandchild.pid}")
mode = sys.argv[2]
if mode == "exit0":
    sys.exit(0)
if mode == "exit3":
    sys.exit(3)
time.sleep(60)
"""

SLEEP = "import time; time.sleep(60)"
IGNORE_TERM = "; ".join(
    (
        "import signal, time",
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)",
        "time.sleep(60)",
    )
)


class _Timeout(BaseException):
    """Stands in for AirflowTaskTimeout, which also derives from BaseException."""


@pytest.fixture
def common(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(DAG_DIR))
    import ingest_common

    monkeypatch.setattr(ingest_common, "REPO", str(tmp_path))
    monkeypatch.setattr(ingest_common, "KILL_GRACE_SECONDS", 1.0)
    (tmp_path / "child.py").write_text(CHILD, encoding="utf-8")
    # The task runs ``python``; point it at this interpreter.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python").symlink_to(sys.executable)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    yield ingest_common
    sys.modules.pop("ingest_common", None)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie still answers signal 0 until its parent reaps it.
    try:
        state = Path(f"/proc/{pid}/stat").read_text().split(")")[-1].split()[0]
    except OSError:
        return False
    return state != "Z"


def _assert_gone(pids: list[int]) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not any(_alive(pid) for pid in pids):
            return
        time.sleep(0.05)
    survivors = [pid for pid in pids if _alive(pid)]
    for pid in survivors:
        os.kill(pid, signal.SIGKILL)
    pytest.fail(f"processes survived: {survivors}")


def _pids(path: Path) -> list[int]:
    return [int(part) for part in path.read_text().split()]


def _raise_timeout(_signum: int, _frame: object) -> None:
    raise _Timeout


def _run_with_alarm(common, tmp_path: Path, mode: str, grandchild: str) -> list[int]:
    pid_file = tmp_path / "pids.txt"
    previous = signal.signal(signal.SIGALRM, _raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, 2.0)
    try:
        with pytest.raises(_Timeout):
            common.run_python_args("child.py", pid_file, mode, grandchild)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
    return _pids(pid_file)


def test_timeout_kills_child_and_grandchild(common, tmp_path: Path) -> None:
    pids = _run_with_alarm(common, tmp_path, "sleep", SLEEP)
    assert len(pids) == 2
    _assert_gone(pids)


def test_grandchild_that_ignores_sigterm_is_killed(common, tmp_path: Path) -> None:
    pids = _run_with_alarm(common, tmp_path, "sleep", IGNORE_TERM)
    _assert_gone(pids)


def test_other_exception_while_waiting_kills_the_group(
    common, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pid_file = tmp_path / "pids.txt"
    real_popen = common.subprocess.Popen

    class _Popen(real_popen):  # type: ignore[misc, valid-type]
        def wait(self, timeout: float | None = None) -> int:
            if timeout is None and not getattr(self, "_raised", False):
                self._raised = True
                deadline = time.monotonic() + 5
                while not pid_file.exists() and time.monotonic() < deadline:
                    time.sleep(0.05)
                msg = "terminated"
                raise KeyboardInterrupt(msg)
            return super().wait(timeout)

    monkeypatch.setattr(common.subprocess, "Popen", _Popen)
    with pytest.raises(KeyboardInterrupt):
        common.run_python_args("child.py", pid_file, "sleep", SLEEP)
    _assert_gone(_pids(pid_file))


def test_non_zero_exit_fails_and_leaves_no_process(common, tmp_path: Path) -> None:
    pid_file = tmp_path / "pids.txt"
    with pytest.raises(RuntimeError, match="entrypoint exited with status 3"):
        common.run_python_args("child.py", pid_file, "exit3", SLEEP)
    _assert_gone(_pids(pid_file))


def test_zero_exit_returns_and_stops_a_leftover_grandchild(
    common, tmp_path: Path
) -> None:
    pid_file = tmp_path / "pids.txt"
    assert common.run_python_args("child.py", pid_file, "exit0", SLEEP) is None
    _assert_gone(_pids(pid_file))


def test_child_runs_in_its_own_session(common, tmp_path: Path, monkeypatch) -> None:
    seen: dict[str, object] = {}
    real_popen = common.subprocess.Popen

    def _spy(command, **kwargs):
        seen.update(kwargs, command=command)
        return real_popen(command, **kwargs)

    monkeypatch.setattr(common.subprocess, "Popen", _spy)
    common.run_python_args("child.py", tmp_path / "pids.txt", "exit0", SLEEP)
    assert seen["start_new_session"] is True
    assert seen["cwd"] == str(tmp_path)
    assert seen["command"][0] == "python"
    assert "shell" not in seen
