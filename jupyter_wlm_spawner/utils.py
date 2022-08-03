import json
import logging
import subprocess
from threading import Timer

DEFAULT_CMD_TIMEOUT = 30  # sec


def parse_connection_file(connection_file_path: str):
    """Parse a Jupyter connection file."""
    with open(connection_file_path, encoding="utf-8") as connection_file:
        return json.load(connection_file)


def get_real_kernel_cmd(argv: list[str]):
    """Get kernel command"""
    ret = " ".join(argv)
    return eval(f'f"""{ret}"""')


def run_cmd(
    cmd: str,
    timeout: int = DEFAULT_CMD_TIMEOUT,
) -> tuple[int, bytes | None, bytes | None, Exception | None]:
    """
    Returns 'rc', 'stdout', 'stderr', 'exception'.
    Where 'exception' is a content of Python exception if any.
    """
    return_code = 255
    stdout, stderr, exception = None, None, None
    logging.debug("Calling: %s", cmd)
    try:
        proc = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        timer = Timer(timeout, lambda p: p.kill(), [proc])
        try:
            timer.start()
            stdout, stderr = proc.communicate()
        except:
            exception = Exception(f"Timeout executing '{cmd}'")
        finally:
            timer.cancel()

        proc.wait()
        return_code = int(proc.returncode)
    except Exception as err:
        exception = err
    return return_code, stdout, stderr, exception
