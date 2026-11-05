import subprocess
import sys

from conftest import ROOT

ENGINE = ("__init__.py", "admin.py", "db.py", "engine.py", "settings.py")


def test_one_copy_of_the_engine():
    assert not (ROOT / "local" / "sampling").exists(), "local/ must use airflow/sampling, not a copy"


def test_engine_never_imports_airflow():
    for name in ENGINE:
        text = (ROOT / "airflow" / "sampling" / name).read_text()
        assert "import airflow" not in text and "from airflow" not in text, name


def test_local_cli_loads_the_engine_without_airflow():
    code = ("import builtins, runpy, sys\n"
            "real = builtins.__import__\n"
            "def guard(name, *a, **k):\n"
            "    if name == 'airflow' or name.startswith('airflow.'):\n"
            "        raise ImportError('no airflow here')\n"
            "    return real(name, *a, **k)\n"
            "builtins.__import__ = guard\n"
            f"sys.argv = ['run_sampling.py', '--help']\n"
            f"runpy.run_path({str(ROOT / 'local' / 'run_sampling.py')!r}, run_name='__main__')\n")
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert done.returncode == 0 and "finalize" in done.stdout, done.stderr
