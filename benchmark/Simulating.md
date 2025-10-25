# Simulating Experiments on Embedded Devices

Users can directly refer to the [User Manual](#user-manual).

## Samsung Galaxy Watch Active 2

| Item                | Specification                                            |
| ------------------- | -------------------------------------------------------- |
| **Processor (SoC)** | Exynos 9110 Dual-core 1.15 GHz (Cortex-A53 architecture) |
| **GPU**             | Mali-T720                                                |
| **Memory (RAM)**    | 1.5 GB (LTE version); 0.75 GB (Bluetooth version)        |
| **Storage (ROM)**   | 4 GB eMMC (approx. 1.4 GB available to users)            |

## What We Need to Simulate

Except for specialized sensors, display, and wireless modules, the rest (CPU, memory, etc.) can be simulated using **QEMU**.

We will not simulate **Tizen**, but instead run **Debian** on the same CPU architecture to approximate the hardware environment.

## Why QEMU Instead of Docker

- Docker can **limit memory, CPU quota/cores, process count, and I/O**, which roughly approximates a **1.5 GB RAM + dual-core** watch environment.
  - However, Docker’s CPU limitation is **logical**, not physical; regardless of settings, `/proc/cpuinfo` still shows the host CPU’s actual core count.
- It **cannot precisely limit CPU frequency** (e.g. 1.15 GHz); only proportional throttling is possible.

---

Check core count:

```bash
nproc
grep -c '^processor' /proc/cpuinfo
```

## Build from Scratch

### 1. Environment Preparation – Install Required Tools

```bash
# Debian/Ubuntu
sudo apt-get update
sudo apt-get install -y \
  qemu-system-aarch64 qemu-utils qemu-efi-aarch64 \
  cloud-image-utils

# libguestfs doesn't support Apple Silicon
sudo apt-get install -y libguestfs-tools
# Avoid "direct mode" issues required by libguestfs on some distributions
echo 'export LIBGUESTFS_BACKEND=direct' >> ~/.bashrc
source ~/.bashrc
```

### 2. Set Up Debian ARM64 Cloud Image

```bash
# Prepare working directory and cloud-init configuration
mkdir -p ~/aarch64-img/work
cd ~/aarch64-img/work

# The official source is often unreliable; use a stable mirror
BACKUP_IMAGE="https://cdimage.debian.org/cdimage/cloud/bookworm/latest/debian-12-generic-arm64.qcow2"
curl -fL -o debian-12-generic-arm64.qcow2 $BACKUP_IMAGE

# Verify file
qemu-img info debian-12-generic-arm64.qcow2

# Create your working image
qemu-img convert -O qcow2 debian-12-generic-arm64.qcow2 gwatch-sim.qcow2
qemu-img resize gwatch-sim.qcow2 +10G
```

### 3. Pre-boot Image Injection

Inject overrides into the image once so that **`sshd` reads them at every boot**. No in-guest execution is needed (safe for cross-architecture).

Enable SSH root password login:

```bash
# 1) Ensure target directory exists
sudo virt-customize -a ~/aarch64-img/work/gwatch-sim.qcow2 --mkdir /etc/ssh/sshd_config.d
# 2) Write override snippet to temporary file
cat > /tmp/zz-cloud-override.conf <<'EOF'
PermitRootLogin yes
PasswordAuthentication yes
ChallengeResponseAuthentication no
UsePAM yes
EOF
# 3) Inject into image
sudo virt-copy-in -a ~/aarch64-img/work/gwatch-sim.qcow2 /tmp/zz-cloud-override.conf /etc/ssh/sshd_config.d/
```

Set the `root` password:

```bash
sudo virt-customize -a ~/aarch64-img/work/gwatch-sim.qcow2 --root-password password:DAT300
```

Tell cloud-init not to look for external data sources:

```bash
cat > /tmp/99-datasource.cfg <<'EOF'
# cloud-init: only use NoCloud (and skip network datasources)
datasource_list: [ NoCloud, None ]
EOF

sudo virt-customize -a ~/aarch64-img/work/gwatch-sim.qcow2 \
  --mkdir /etc/cloud/cloud.cfg.d

sudo virt-copy-in -a ~/aarch64-img/work/gwatch-sim.qcow2 \
  /tmp/99-datasource.cfg /etc/cloud/cloud.cfg.d/
```

Copy `chdb_cli.cpp` into `/root/`:

```bash
CHDB_CLI_CPP_PATH="/home/xuan/dataset_analytics/benchmark/chdb_cli/chdb_cli.cpp"
sudo virt-copy-in -a ~/aarch64-img/work/gwatch-sim.qcow2 \
  $CHDB_CLI_CPP_PATH /root/
```

### 4. Create cloud-init Seed

Prepare directory and configuration:

```bash
mkdir -p ~/aarch64-img/seed
cd ~/aarch64-img/seed
```

Create `user-data.yaml`, which installs dependencies on first boot:

```yaml
#cloud-config
package_update: true
packages:
  - ca-certificates
  - curl
  - g++
  - python3
  - python3-psutil
  - python3-pandas
  - python3-matplotlib
  - python3-yaml
  - sqlite3

runcmd:
  - echo "alias python=python3" >> /root/.bashrc

  - curl -fsSL https://install.duckdb.org | sh
  - bash -lc 'echo export PATH=\"$HOME/.duckdb/cli/latest:\$PATH\" >> /root/.profile'
  - bash -lc 'echo export PATH=\"$HOME/.duckdb/cli/latest:\$PATH\" >> /root/.bashrc'

  - curl -fsSL https://lib.chdb.io | bash || true
  - mkdir -p /root/.local/bin
  - bash -lc 'set -e; g++ -o /root/.local/bin/chdb_cli /root/chdb_cli.cpp -lchdb -L/usr/local/lib'
  - bash -lc 'echo export PATH=\"\$PATH:/root/.local/bin\" >> /root/.profile'
  - bash -lc 'echo export PATH=\"\$PATH:/root/.local/bin\" >> /root/.bashrc'

final_message: "✅ First boot provisioning completed"

power_state:
  mode: poweroff
```

Create empty `meta-data`:

```bash
: > meta-data
```

Generate seed image:

```bash
cloud-localds -v seed.iso user-data.yaml meta-data
```

Verify output:

```bash
ls -lh seed.iso
```

### 5. First Boot (cloud-init Executes and Powers Off)

```bash
cd ~/aarch64-img/work
BIOS=$(dpkg -L qemu-efi-aarch64 | grep QEMU_EFI.fd | head -n1)

qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 -smp 2 -m 1536 \
  -bios "${BIOS}" \
  -drive if=virtio,file=gwatch-sim.qcow2,format=qcow2 \
  -cdrom ../seed/seed.iso \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -nographic
```

Wait a few minutes until you see logs like:

```bash
Cloud-init ... finished at ...
Reached target Power-Off
```

The VM will shut down automatically.

### 6. Second Boot (without seed.iso)

Disable cloud-init to speed up subsequent boots:

```bash
sudo virt-customize -a ~/aarch64-img/work/gwatch-sim.qcow2 --touch /etc/cloud/cloud-init.disabled
```

---

```bash
qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 -smp 2 -m 1536 \
  -bios "${BIOS}" \
  -drive if=virtio,file=gwatch-sim.qcow2,format=qcow2 \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -nographic -serial mon:stdio
```

Login to verify environment:

```bash
# Verify Python environment and tools
python3 -c "import pandas,matplotlib,psutil,yaml; print('Python env OK')"
duckdb --version
sqlite3 --version
chdb_cli --help
```

### 7. Test SSH Login (on Host)

Open a new terminal:

```bash
nc -vz 127.0.0.1 2222
# If succeeded, try SSH:
ssh -o StrictHostKeyChecking=no -p 2222 root@localhost
# Password: DAT300
```

Successful login shows:

```bash
root@debian:~#
```

### 8. Disk Compression

```bash
qemu-img convert -O qcow2 -c gwatch-sim.qcow2 gwatch-sim.comp.qcow2
```

### About Importing Other Files

There are project code, database files, SQL files, and YAML configuration files to handle.

Due to project iteration, it is recommended to use several scripts for each category.

Check if the target directory exists:

```bash
sudo virt-customize -a gwatch-sim.qcow2 --mkdir /root/benchmark
```

Project code:

```bash
sudo virt-copy-in -a gwatch-sim.qcow2 \
  ./cli ./config ./consts ./models ./service ./util ./run_experiments.py \
  /root/benchmark/
```

Database files:

```bash
sudo virt-copy-in -a gwatch-sim.qcow2 ./db_vs14 /root/benchmark/
```

SQL files and YAML configuration files:

```bash
sudo virt-copy-in -a gwatch-sim.qcow2 \
  ./config_yaml ./queries \
  /root/benchmark/
```

### Convenient Operations

Redirect both the serial console and QEMU monitor to the current terminal I/O:

```bash
-serial mon:stdio
```

Shutdown:

```bash
poweroff
```

Force quit:

```bash
sudo pkill -f qemu-system-aarch64
```

Remove SSH host entry:

```bash
ssh-keygen -f '/home/xuan/.ssh/known_hosts' -R '[localhost]:2222'
```

Compress/Decompress disk:

```bash
# Compress
qemu-img convert -O qcow2 -c gwatch-sim.qcow2 gwatch-sim.comp.qcow2

# Decompress
qemu-img convert -O qcow2 gwatch-sim.comp.qcow2 gwatch-sim.qcow2
```

> The compressed version is for storage/distribution only — **not recommended for direct execution** (slightly slower). For performance experiments, use the decompressed version.

Upload/download files:

```bash
# Upload a file to the image
sudo virt-copy-in -a <image_path> <host_file_path> <guest_target_path>

# Upload a directory to the image
sudo virt-copy-in -a <image_path> <host_directory> <guest_target_path>

# Extract a single file from the image
sudo virt-copy-out -a <image_path> <guest_file_path> <host_target_path>

# Extract a directory from the image
sudo virt-copy-out -a <image_path> <guest_directory> <host_target_path>
```

## User Manual

### Roles of the Virtual Machine

The virtual machine runs on the ARM64 platform, emulated entirely in software via QEMU’s TCG.

A Python environment is preinstalled, along with CLI tools for three database engines.

The virtual machine **does not** handle `create`, `validate`, or final `analyze` stages. It only performs the **`run_experiments`** step.

### Workflow

1. Install required tools + download the image
2. Decompress image
3. Create directory, upload project code and database files (SQL/YAML config)
4. Backup after first upload
5. Start the VM
6. SSH login and run an experiment
7. Download results
8. Shut down the VM
9. Repeat steps 5–8 for each experiment

[Simulating Workflow](Simulating_Workflow.md) outlines the 1st to 8th steps of the simulating workflow.

### Install Required Tools

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

### Download and Decompress Image

Download image:

```bash
pip install gdown
QCOW2="https://drive.google.com/uc?id=1ciPZ9iOy17D2KfCcIqjPRBcRj8OxU7Z8"
gdown $QCOW2
```

Decompress:

```bash
qemu-img convert -O qcow2 gwatch-sim.comp.qcow2 gwatch-sim.qcow2
```

### Upload/Download Files

Note: Modify the `chdb` path in `config_yaml/config.yaml` to:

```yaml
engine_paths:
  duckdb: duckdb
  sqlite: sqlite3
  chdb: chdb_cli
```

Because all these executables are preinstalled and included in the `PATH`.

#### Use SFTP

Interactive file transfer:

```bash
# need to set fingerprint for first run
sftp -P 2222 root@localhost

# Upload a directory
# (./dataset/) local -> (./root/) remote
sftp> put -r ./dataset /root/

# Download a directory
# (/root/results) remote -> (./backup/) local
sftp> get -r /root/results ./backup/
```

Non-interactive transfer:

```bash
# Upload
sftp -P 2222 root@localhost:/remote/path/ <<< $"put -r /local/path/dir"

# Download
sftp -P 2222 root@localhost <<< $"get -r /root/data ./backup/"
```

---

Upload all code related to `run_experiments.py` under `benchmark`:

```bash
# in benchmark dir
sftp -P 2222 root@localhost
sftp>
mkdir benchmark
put -r ./cli /root/benchmark/
put -r ./config /root/benchmark/
put -r ./consts /root/benchmark/
put -r ./models /root/benchmark/
put -r ./service /root/benchmark/
put -r ./util /root/benchmark/
put ./run_experiments.py /root/benchmark/
```

Upload database files, YAML configs, SQL files:

```bash
sftp -P 2222 root@localhost
sftp>
put -r ./db_vs14 /root/benchmark/
put -r ./config_yaml /root/benchmark/
put -r ./queries /root/benchmark/
```

Download experiment results (VM must be running):

```bash
# In benchmark directory
mkdir -p results

# Experiment configuration name
CONFIG_NAME="dev"
sftp -P 2222 root@localhost <<< $"get -r /root/benchmark/results/$CONFIG_NAME ./results/$CONFIG_NAME"
```

#### Use `libguestfs`

> `libguestfs` does **not** require the VM to be running!

Create directory:

```bash
sudo virt-customize -a gwatch-sim.qcow2 --mkdir /root/benchmark
```

Upload project code:

```bash
sudo virt-copy-in -a gwatch-sim.qcow2 \
  ./cli ./config ./consts ./models ./service ./util ./run_experiments.py \
  /root/benchmark/
```

Upload database files:

```bash
sudo virt-copy-in -a gwatch-sim.qcow2 ./db_vs14 /root/benchmark/
```

Upload SQL and YAML config files:

```bash
sudo virt-copy-in -a gwatch-sim.qcow2 \
  ./config_yaml ./queries \
  /root/benchmark/
```

Download experiment results (**VM must be shut down**, not recommended):

```bash
# In benchmark directory
mkdir -p results

# Experiment configuration name
CONFIG_NAME="dev"
sudo virt-copy-out -a gwatch-sim.qcow2 /root/benchmark/results/$CONFIG_NAME ./results/$CONFIG_NAME
```

### Start/Stop the VM

> For macOS, BIOS path should be `/opt/homebrew/share/qemu/edk2-aarch64-code.fd`.
> For macOS with Apple Silicon, `-accel hvf` can be used instead of `-accel tcg,thread=multi,tb-size=2048`.
> If the default SSH port is occupied, change `hostfwd=tcp::2223-:22`.

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

Login credentials:

- **Username:** root
- **Password:** DAT300

---

You can safely power off:

```bash
poweroff
```

### SSH Connection

```bash
ssh -p 2222 root@localhost
```

### Backup After First Upload

> A recommended practice is to start fresh for each config to avoid interference.
> In practice, once the code and database files are stable, you can start from this checkpoint.

After uploading project code and database files (SQL/YAML), shut down and back up:

```bash
cp gwatch-sim.qcow2 gwatch-sim-checkpoint.qcow2
```

For subsequent experiments, use the `snapshot=on` option. This avoids re-uploading data and keeps experiments isolated.

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
