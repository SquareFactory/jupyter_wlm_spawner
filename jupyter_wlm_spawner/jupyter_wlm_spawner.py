import argparse
import atexit
import json
import os
import subprocess
import logging
from threading import Timer
from Crypto.PublicKey import ECC

from jupyter_client.kernelspec import KernelSpecManager

DEFAULT_CMD_TIMEOUT = 30  # sec


def run_cmd(
    cmd: str, timeout=DEFAULT_CMD_TIMEOUT
) -> tuple[int, bytes | None, bytes | None, Exception | None]:
    """
    Returns 'rc', 'stdout', 'stderr', 'exception'.
    Where 'exception' is a content of Python exception if any.
    """
    return_code = 255
    stdout, stderr, exception = None, None, None
    logging.debug(f"Calling: {cmd}")
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
    except Exception as e:
        exception = e
    return return_code, stdout, stderr, exception


class WLMSpawnerError(RuntimeError):
    """Workload Manager Runtime Error."""


class WLMSpawner:
    def __init__(self):
        self.args = self.parse_arguments()
        self.initialize_keys()
        self.connection = self.parse_connection_file(self.args.connection_file)
        self.spawn = eval(f"self._spawn_{self.args.scheduler}")
        self.kernel_spec = KernelSpecManager().get_kernel_spec(
            self.args.kernel
        )

    def parse_arguments(self):
        """Parse program arguments"""
        user = os.environ.get("USER")
        if user is None:
            raise WLMSpawnerError("$USER is none")

        home = os.environ.get("HOME")
        if home is None:
            raise WLMSpawnerError("$HOME is none")

        private_key_path = f"{home}/.ssh/{user}_jupslurm"
        parser = argparse.ArgumentParser(
            description="WLM spawner",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "-f", "--connection-file", help="Connection file", required=True
        )
        parser.add_argument(
            "--srun",
            help="remote srun executable (accepts arguments)",
            default="srun",
        )
        parser.add_argument(
            "--scancel",
            help="scancel executable (accepts arguments)",
            default="scancel",
        )
        parser.add_argument(
            "--scontrol",
            help="scontrol executable (accepts arguments)",
            default="scontrol",
        )
        parser.add_argument(
            "-s",
            "--scheduler",
            choices=["slurm", "sge"],
            default="slurm",
            help="Scheduler type",
        )
        parser.add_argument(
            "-o",
            "--wlm-options",
            default="",
            help="Additional options for workload manager",
        )
        parser.add_argument(
            "-k", "--kernel", default="python3", help="Kernel to spawn"
        )
        parser.add_argument(
            "--keyfile",
            default=private_key_path,
            help="SSH private key file",
        )
        parser.add_argument(
            "-e", "--env-commands", default="", help="Kernel to spawn"
        )

        return parser.parse_args()

    def parse_connection_file(self, connection_file):
        """Parse a Jupyter connection file."""
        with open(connection_file, encoding="utf-8") as f:
            return json.load(f)

    def get_scontrol_job_field(self, jobid: int, field: str):
        """Fetch and parse slurm job info."""
        res = None
        field += "="
        what_size = len(field)
        return_code, stdout, stderr, exception = run_cmd(
            f"{self.args.scontrol} show job {jobid}"
        )
        if exception is not None:
            logging.warning(exception)

        if return_code:
            raise WLMSpawnerError(
                f"Unable to get job status. Jobid: {jobid}; stderr: {stderr}"
            )
        show_job = str(stdout).split()
        for rec in show_job:
            if rec[:what_size] == field:
                res = rec[what_size:]
        if res is None:
            raise WLMSpawnerError(
                f"Unable to get job field {field} for job {jobid}"
            )
        return res

    def _spawn_sge(self):
        # TODO
        raise NotImplementedError

    def _spawn_slurm(self):
        # With -I don't wait inifinite time for allocation
        salloc_cmd = [
            *self.args.salloc.split(),
            f"-I{DEFAULT_CMD_TIMEOUT}",
            *self.args.wlm_options.split(),
        ]

        salloc_proc = subprocess.Popen(
            salloc_cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )

        # read first line
        salloc_line = ""
        if salloc_proc.stderr is not None:
            salloc_line = str(salloc_proc.stderr.readline())

        # last element should be job number:
        try:
            jobid = int(salloc_line.split()[-1])
        except ValueError as error:
            # didn't retur job number. Error.
            raise WLMSpawnerError(
                f"Unable to get jobid. Error on allocation: {salloc_line}"
            ) from error

        # delete job on exit
        atexit.register(
            subprocess.Popen,
            [*self.args.scancel.split(), str(jobid)],
            shell=False,
        )

        # check if job in RUNNING state
        job_state = "UNKNOWN"
        while job_state != "RUNNING":
            job_state = self.get_scontrol_job_field(jobid, "JobState")
            if salloc_proc.poll():
                # -I in salloc did it's job
                raise WLMSpawnerError("Unable to get allocation.")

        # job in RUNNING state
        # get batch node
        batch_host = self.get_scontrol_job_field(jobid, "BatchHost")[:-2]

        # Forward ports
        ssh_forwarding = [
            "ssh",
            "-i",
            self.args.keyfile,
            "-L",
            f"{self.connection['shell_port']}:localhost:{self.connection['shell_port']}",
            "-L",
            f"{self.connection['iopub_port']}:localhost:{self.connection['iopub_port']}",
            "-L",
            f"{self.connection['stdin_port']}:localhost:{self.connection['stdin_port']}",
            "-L",
            f"{self.connection['control_port']}:localhost:{self.connection['control_port']}",
            "-L",
            f"{self.connection['hb_port']}:localhost:{self.connection['hb_port']}",
            "-N",
            batch_host,
        ]

        logging.debug(f"Calling: {ssh_forwarding}")

        ssh_proc = subprocess.Popen(
            ssh_forwarding,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
        # drop ssh tunnels on exit
        atexit.register(ssh_proc.terminate)

        # Copy kernel file to host
        dest_dir = "/".join(self.args.connection_file.split("/")[:-1])

        # create dir to store connection file
        return_code, _, stderr, exception = run_cmd(
            f"ssh -i {self.args.keyfile} {batch_host} mkdir -p {dest_dir}"
        )
        if exception is not None:
            logging.warning(exception)
        if return_code:
            raise WLMSpawnerError(
                f"Unable to create dir to store connection_file: stderr: {stderr}"
            )

        # chmod
        return_code, _, stderr, exception = run_cmd(
            f"ssh -i {self.args.keyfile} {batch_host} chmod 1700 {dest_dir}"
        )
        if exception is not None:
            logging.warning(exception)
        if return_code:
            raise WLMSpawnerError(
                f"Unable to chmod on dir to store connection_file: stderr: {stderr}"
            )

        # copy connection file
        return_code, _, stderr, exception = run_cmd(
            f"scp -i {self.args.keyfile} -pr {self.args.connection_file} {batch_host}:{self.args.connection_file}"
        )
        if exception is not None:
            logging.warning(exception)
        if return_code:
            raise WLMSpawnerError(
                f"Unable to scp connection_file: stderr: {stderr}"
            )

        # run real kernel
        kernel_cmd = self.get_real_kernel_cmd()
        env_commands = self.args.env_commands.strip()[1:-1]
        kernel_script = "set -e"
        kernel_script = "set -x"
        kernel_script += f"{env_commands}\n"
        kernel_script += (
            f"{self.args.srun} -N 1 -E -w {batch_host} {kernel_cmd}\n"
        )
        _ = salloc_proc.communicate(kernel_script)
        salloc_proc.wait()

        # Cleanup. Delete unnneded connection file
        return_code, _, stderr, exception = run_cmd(
            f"ssh -i {self.args.keyfile} {batch_host} rm -f {self.args.connection_file}"
        )
        if exception is not None:
            logging.warning(exception)
        if return_code:
            raise WLMSpawnerError(
                f"Unable to delete connection file: stderr: {stderr}"
            )

    def initialize_keys(self):
        """Generate and use SSH keys"""
        if not os.path.exists(self.args.keyfile):
            logging.warning(
                f"keyfile doesn't exists. Generating {self.args.keyfile}"
            )

            # Generate and write private key
            private_key_path = self.args.keyfile
            private_key = ECC.generate(curve="ed25519")
            with open(private_key_path, "w", encoding="utf-8") as pk_file:
                os.chmod(private_key_path, 0o600)
                pk_file.write(str(private_key.export_key(format="PEM")))

            # Generate and write public key
            public_key_path = f"{private_key_path}.pub"
            public_key = private_key.public_key().export_key(format="OpenSSH")
            with open(public_key_path, "w", encoding="utf-8") as pub_file:
                os.chmod(public_key_path, 0o600)
                pub_file.write(str(public_key))

            # Add public key to authorized_keys
            home = os.environ.get("HOME")
            if not os.path.exists(f"{home}/.ssh"):
                os.makedirs(f"{home}/.ssh", mode=0o700)
            with open(
                f"{home}/.ssh/authorized_keys", "a", encoding="utf-8"
            ) as authorized_keys:
                os.chmod(f"{home}/.ssh/authorized_keys", 0o600)
                authorized_keys.write(str(public_key))

    def get_real_kernel_cmd(self):
        ret = " ".join(self.kernel_spec.argv)
        return eval(f'f"""{ret}"""')


def main():
    """Main entrypoint."""
    wlm_spawner = WLMSpawner()
    wlm_spawner.spawn()
