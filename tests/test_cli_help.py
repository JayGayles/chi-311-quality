import subprocess
import sys

def _run_ok(args):
    p = subprocess.run([sys.executable] + args, capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr

def test_peek_shows_help():
    code, out = _run_ok(["-m", "src.peek", "--help"])
    assert code == 0
    assert "--source" in out

def test_explore_shows_help():
    code, out = _run_ok(["-m", "src.explore_311", "--help"])
    assert code == 0
    assert "--source" in out

def test_fetch_shows_help():
    code, out = _run_ok(["-m", "src.fetch", "--help"])
    assert code == 0
    assert "--days" in out