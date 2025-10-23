# Simulating Experiments on Embedded Devices

Install the required packages (including ARM UEFI firmware):

```bash
sudo apt-get update
sudo apt-get install -y qemu-system-aarch64 qemu-utils qemu-efi-aarch64
```

Prepare the disk file: [`gwatch-sim.qcow2`](https://drive.google.com/uc?id=1UcJyO3E7B0OYBxhB3ndwwxAbd5YAv9s1)

```bash
pip install gdown
gdown https://drive.google.com/uc?id=1UcJyO3E7B0OYBxhB3ndwwxAbd5YAv9s1
```

Backup the disk file:

```bash
cp gwatch-sim.qcow2 gwatch-sim-checkpoint.qcow2
```

Boot from the disk:

> For macOS, the path should be: `/opt/homebrew/share/qemu/edk2-aarch64-code.fd` — modify accordingly.
>
> Network port forwarding: if the port is occupied, change to `hostfwd=tcp::2223-:22`.

```bash
qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 \
  -smp 2 -m 1536 \
  -bios /usr/share/qemu-efi-aarch64/QEMU_EFI.fd \
  -drive if=virtio,file=gwatch-sim-checkpoint.qcow2,format=qcow2,cache=writeback \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -nographic
```

Login credentials:

**Username:** root

**Password:** DAT300

------

A Python environment is already provided, along with the CLI for three database engines and all benchmark code related to `run_experiments.py`.

You only need to upload your **database files**, **YAML configuration files**, and **SQL files**:

```bash
# Upload database (first time)
# Later updates only require uploading config or SQL files
sftp -P 2222 root@localhost

sftp>
put -r ./db_vs14 /root/benchmark/
put -r ./config_yaml /root/benchmark/
put -r ./queries /root/benchmark/
```

Note: You must modify the `chdb` path in `config_yaml/config.yaml` to:

```yaml
engine_paths:
  duckdb: duckdb
  sqlite: sqlite3
  chdb: chdb_cli
```

This is because all these executables are already installed in the system and added to the `PATH`.

------

Export experimental results:

```bash
# In the benchmark directory
mkdir -p results

# Experiment configuration name
CONFIG_NAME="dev"
sftp -P 2222 root@localhost <<< $"get -r /root/benchmark/results/$CONFIG_NAME ./results/$CONFIG_NAME"
```

## Recommendation

After uploading the database files, YAML configuration, and SQL files for the first time, you can safely power off the system (`poweroff`).

For subsequent experiments, use the `snapshot=on` option:

```bash
qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 \
  -smp 2 -m 1536 \
  -bios /usr/share/qemu-efi-aarch64/QEMU_EFI.fd \
  -drive if=virtio,file=gwatch-sim-checkpoint.qcow2,format=qcow2,cache=writeback,snapshot=on \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -nographic
```

This allows you to avoid re-uploading the database each time while ensuring that different experiments do not interfere with one another.

## Responsibilities of the Virtual Machine

The virtual machine is **not** responsible for `create`, `validate`, or final `analyze` stages.

It only handles the **`run_experiments`** step.
