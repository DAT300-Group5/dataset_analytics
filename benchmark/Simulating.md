# Simulating Experiments on Embedded Devices

Users can directly refer to the [Simulating Workflow](Simulating_Workflow.md).

## Samsung Galaxy Watch Active 2

| Item                | Specification                                            |
| ------------------- | -------------------------------------------------------- |
| **Processor (SoC)** | Exynos 9110 Dual-core 1.15 GHz (Cortex-A53 architecture) |
| **GPU**             | Mali-T720                                                |
| **Memory (RAM)**    | 1.5 GB (LTE version); 0.75 GB (Bluetooth version)        |
| **Storage (ROM)**   | 4 GB eMMC (approx. 1.4 GB available to users)            |
| **OS**              | Tizen                                                    |

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
sudo virt-copy-in -a ~/aarch64-img/work/gwatch-sim.qcow2 \
  /tmp/zz-cloud-override.conf /etc/ssh/sshd_config.d/
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

  # - curl -fsSL https://install.duckdb.org | sh
  - bash -lc 'echo export PATH=\"/root/.duckdb/cli/latest:\$PATH\" >> /root/.profile'
  - bash -lc 'echo export PATH=\"/root/.duckdb/cli/latest:\$PATH\" >> /root/.bashrc'

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
# Install DuckDB here
curl -fsSL https://install.duckdb.org | sh

# Verify Python environment and tools
python -c "import pandas,matplotlib,psutil,yaml; print('Python env OK')"
duckdb --version
sqlite3 --version
chdb_cli
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

## Appendix

### QEMU – The Image Is Just a Disk

In QEMU, parameters like:

```bash
-smp 2 -m 1536 -machine virt -cpu cortex-a53 ...
```

are **runtime configurations**, not permanently written into the virtual disk image (`.qcow2`).

In the QEMU model:

- `.qcow2` or `.img` = the guest system’s **disk content**
- It stores the OS, files, and user data
- But **CPU, memory, motherboard, NIC, BIOS** are **externally specified**

This means QEMU **decouples** “hardware configuration” (CPU, memory, devices) from “disk content.”

You can boot the same disk image under different virtual hardware configurations (e.g. 1 core / 512 MB vs. 4 cores / 2 GB).

| Feature        | VirtualBox / VMware                       | QEMU                                        |
| -------------- | ----------------------------------------- | ------------------------------------------- |
| Config storage | Saved in `.vbox` / `.vmx` (CPU, RAM, NIC) | User-defined via CLI                        |
| Disk format    | `.vdi` / `.vmdk`                          | `.qcow2` / `.img`                           |
| Launch method  | GUI-based                                 | Command-line                                |
| Flexibility    | Fixed                                     | Extremely flexible (for automation/testing) |

QEMU’s design advantages:

1. **Flexibility** — test the same image under different virtual hardware.
2. **Scriptability** — fully automatable for CI or batch experiments.
3. **Portability** — `.qcow2` is self-contained and transferable.

### TCG Software Emulation

QEMU has two fundamentally different operation modes:

| Mode                                         | Purpose        | Mechanism                                                        | CPU Type Customizable |
| -------------------------------------------- | -------------- | ---------------------------------------------------------------- | --------------------- |
| 🧠 **TCG (Tiny Code Generator)**              | Full emulation | Translates guest instructions into host instructions dynamically | ✅ Yes                 |
| ⚙️ **Hardware Virtualization (KVM/HVF/WHPX)** | Virtualization | Executes guest instructions directly on host CPU                 | ❌ No                  |

Only **TCG mode** simulates detailed **CPU microarchitecture**. For example:

| Feature               | Real Cortex-A53 | QEMU TCG (`-cpu cortex-a53`) | HVF/KVM         |
| --------------------- | --------------- | ---------------------------- | --------------- |
| ISA (AArch64)         | ✅               | ✅                            | ✅ (via host)    |
| LSE (Atomics)         | ❌               | ❌                            | Depends on host |
| PMU/Timer             | A53-specific    | Emulated A53 PMU             | Host PMU        |
| Performance           | Native          | Slow (software emulated)     | Near-native     |
| Architecture fidelity | ✅               | ✅                            | ❌               |

In short:

- **TCG = Interpreter obeying your `-cpu` definition**
- **HVF/KVM = Pass-through executor using host CPU directly**

When you use `-cpu cortex-a53`, QEMU defines:

- Supported instruction sets (AArch64/AArch32)
- Enabled/disabled extensions (LSE, CRC, Crypto, PMU)
- Register behavior
- Exception levels
- Timer precision and system control registers

Thus, the environment is **logically equivalent to a real Cortex-A53**, though slower.
