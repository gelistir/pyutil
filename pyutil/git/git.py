import subprocess


def git_version(cwd=None):
    return subprocess.check_output(args=["git", "describe"], cwd=cwd).decode()

