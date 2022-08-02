# jupyter-wlm-spawner

Wrapper for spawning remote iPython/Jupyter kernel via HPC workload managers.

For now only SLURM is supported.

See kernel.json for example definition of the jupyter kernel

## Explanation

1. Jupyter calls the kernel

2. The kernel calls `salloc` and get an allocation on a SLURM node

3. After getting the allocation, the kernel fetches the SLURM node IP address and use SSH forwarding to the compute node according to the connection file.
   (localhost:shell_port -> batchhost:shell_port)
   (localhost:iopub_port -> batchhost:iopub_port)
   (localhost:stdin_port -> batchhost:stdin_port)
   (localhost:control_port -> batchhost:control_port)
   (localhost:hb_port -> batchhost:hb_port)

   1. It copies the connection file at the root of the host

   2. Then, it runs the kernel command and the ipykernel is now running thanks to the connection file

   3. The user can now use the IPython kernel remotely
