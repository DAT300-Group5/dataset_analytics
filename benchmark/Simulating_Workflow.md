# Simulating Workflow

1. Install required tools + download the image
2. Decompress image
3. Create directory, upload project code and database files (SQL/YAML config)
4. Backup after first upload
5. Start the VM
6. SSH login and run an experiment
7. Download results
8. Shut down the VM
9. Repeat steps 5–8 for each experiment

This document outlines the 1st to 8th steps of the simulating workflow.

## 1 Install Required Tools

```bash
# Debian/Ubuntu
sudo apt-get update
sudo apt-get install -y qemu-system-aarch64 qemu-utils qemu-efi-aarch64

# libguestfs doesn't support Apple Silicon
sudo apt-get install -y libguestfs-tools
# Avoid "direct mode" issues required by libguestfs on some distributions
echo 'export LIBGUESTFS_BACKEND=direct' >> ~/.bashrc
source ~/.bashrc
```

## 2 Download image

```bash
# in project root dir
pip install gdown
QCOW2="https://drive.google.com/uc?id=1XKFniKNfB4hkA020rI3anY5TXYwm7Ipa"
gdown $QCOW2
```

## 3 Decompress image

```bash
# in project root dir
qemu-img convert -O qcow2 gwatch-sim.comp.qcow2 gwatch-sim.qcow2
```

## 3 Create directory, upload project code and database files (SQL/YAML config)

Note: Modify the `chdb` path in `config_yaml/config.yaml` to:

```yaml
engine_paths:
  duckdb: duckdb
  sqlite: sqlite3
  chdb: chdb_cli
```

The rest should be also prepared:

```bash
# in benchmark dir
chmod +x ./prepare.sh
# in your conda env
sudo ./prepare.sh
```

At this step, you can choose to use SFTP or `libguestfs`.

### Use SFTP

Start the VM first:

```bash
BIOS=$(dpkg -L qemu-efi-aarch64 | grep QEMU_EFI.fd | head -n1)
FILE="gwatch-sim.qcow2"

qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 \
  -smp 2 -m 1536 \
  -bios "${BIOS}" \
  -drive if=virtio,file="${FILE}",format=qcow2,cache=writeback \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -nographic
```

Simply upload benchmark dir:

```bash
# in project root dir
sftp -P 2222 root@localhost
sftp>
put -r ./benchmark /root/
```

Then power off:

```bash
poweroff
```

### Use `libguestfs`

Simply upload benchmark dir:

```bash
# in project root dir
sudo virt-copy-in -a gwatch-sim.qcow2 ./benchmark /root/
```

## 4 Backup after first upload

```bash
cp gwatch-sim.qcow2 gwatch-sim-checkpoint.qcow2
```

## 5 Start the VM

For subsequent experiments, use the `snapshot=on` option and `gwatch-sim-checkpoint.qcow2`!

```bash
BIOS=$(dpkg -L qemu-efi-aarch64 | grep QEMU_EFI.fd | head -n1)
FILE="gwatch-sim-checkpoint.qcow2"

qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 \
  -smp 2 -m 1536 \
  -bios /usr/share/qemu-efi-aarch64/QEMU_EFI.fd \
  -drive if=virtio,file="${FILE}",format=qcow2,cache=writeback,snapshot=on \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -nographic
```

## 6 SSH login and run an experiment

```bash
ssh -p 2222 root@localhost
```

Login credentials:

- **Username:** root
- **Password:** DAT300

```bash
cd benchmark/
python run_experiments.py --env dev
```

## 7 Download results

Download experiment results (VM must be running):

```bash
# in benchmark dir
mkdir -p results

# Experiment configuration name
CONFIG_NAME="anomaly"
sftp -P 2222 root@localhost <<< $"get -r /root/benchmark/results/$CONFIG_NAME ./results/$CONFIG_NAME"
```

## 8 Shut down the VM

```bash
poweroff
```
