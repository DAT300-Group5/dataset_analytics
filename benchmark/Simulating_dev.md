# Simulating Experiments on Embedded Devices (Dev Doc)

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

## Booting / Disk Installation Commands

Install required packages (including ARM UEFI firmware):

```bash
sudo apt-get update
sudo apt-get install -y qemu-system-aarch64 qemu-utils qemu-efi-aarch64
```

Prepare the image and disk:

```bash
# 1. Download Debian ARM64 netinst image
wget https://cdimage.debian.org/debian-cd/current/arm64/iso-cd/debian-13.1.0-arm64-netinst.iso

# 2. Create a 20 GB virtual disk
qemu-img create -f qcow2 gwatch-sim.qcow2 20G
```

A better image might exist — e.g. **Yocto Project / OpenEmbedded** or **Buildroot** — but these require additional learning time.

### Boot for Installation (with ISO)

Find the BIOS firmware:

```bash
dpkg -L qemu-efi-aarch64 | grep QEMU_EFI.fd
```

For macOS, it should be at:
 `/opt/homebrew/share/qemu/edk2-aarch64-code.fd` — modify accordingly.

Network port forwarding: if the port is in use, replace `hostfwd=tcp::2222-:22` with `hostfwd=tcp::2223-:22`.

```bash
qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 \
  -smp 2 -m 1536 \
  -bios /usr/share/qemu-efi-aarch64/QEMU_EFI.fd \
  -drive if=virtio,file=gwatch-sim.qcow2,format=qcow2,cache=writeback \
  -cdrom debian-13.1.0-arm64-netinst.iso \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -boot d \
  -nographic
```

Follow the installer steps and set the `root` password to **DAT300**.

### After Installation (Boot from Disk)

> The `-drive if=virtio` option defines the disk.
>
> During installation, use `-cdrom` and `-boot d`, but **remove them** when booting from the installed disk.

```bash
qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 \
  -smp 2 -m 1536 \
  -bios /usr/share/qemu-efi-aarch64/QEMU_EFI.fd \
  -drive if=virtio,file=gwatch-sim.qcow2,format=qcow2,cache=writeback \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -nographic
```

---

In the QEMU / Virt environment, **the entire system state resides in the disk file**, e.g., `gwatch-sim.qcow2`.

This file serves as the system’s **virtual hard drive**, containing:

- The installed Linux system
- All user files
- Installed software and configurations

You only need to boot from this disk file afterward.

---

To resume from a specific checkpoint, make a backup.

Backup disk file (permanent snapshot):

```bash
cp gwatch-sim.qcow2 gwatch-sim-checkpoint.qcow2
```

To restore from that checkpoint:

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

For temporary experiments, use `snapshot=on` (discard changes after shutdown):

```bash
qemu-system-aarch64 \
  -machine virt \
  -accel tcg,thread=multi,tb-size=2048 \
  -cpu cortex-a53 \
  -smp 2 -m 1536 \
  -bios /usr/share/qemu-efi-aarch64/QEMU_EFI.fd \
  -drive if=virtio,file=gwatch-sim.qcow2,format=qcow2,cache=writeback,snapshot=on \
  -nic user,model=virtio-net-pci,hostfwd=tcp::2222-:22 \
  -nographic
```

## SSH and File Transfer

Requirements:

> 1. Before experiments: **import data (large files)**
> 2. After experiments: **export results**

For such staged data exchange, **stable import/export** via SSH/SFTP is preferred over real-time synchronization mechanisms like `virtio-fs`.

---

Enable SSH access:

```bash
echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
systemctl restart ssh
grep PermitRootLogin /etc/ssh/sshd_config
```

Connect from host:

```bash
ssh -p 2222 root@localhost
```

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

Contents to include in a shared image:

- All code related to `run_experiments.py` under `benchmark`:

```bash
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

- Database files, YAML configs, SQL files — upload separately by users:

```bash
sftp -P 2222 root@localhost
sftp>
put -r ./db_vs14 /root/benchmark/
put -r ./config_yaml /root/benchmark/
put -r ./queries /root/benchmark/
```

Files to download (after each experiment):

```bash
mkdir -p results
CONFIG_NAME="dev"
sftp -P 2222 root@localhost <<< $"get -r /root/benchmark/results/$CONFIG_NAME ./results/$CONFIG_NAME"
```

## Installation and Setup

Python 3 is preinstalled; make `python` alias to `python3`:

```bash
echo "alias python=python3" >> ~/.bashrc
source ~/.bashrc
```

Install dependencies:

```bash
apt update
apt install -y \
  python3-psutil \
  python3-pandas \
  python3-matplotlib \
  python3-yaml
```

Install `sqlite3` and `duckdb`:

```bash
apt install -y sqlite3
apt install -y curl
curl https://install.duckdb.org | sh
echo 'export PATH="$HOME/.duckdb/cli/latest:$PATH"' >> ~/.profile
source ~/.profile
```

Install `chdb`:

```bash
apt install -y g++

curl -sL https://lib.chdb.io | bash

mkdir -p ~/.local/bin
# Transfer chdb_cli.cpp via sftp
g++ -o ~/.local/bin/chdb_cli chdb_cli.cpp -lchdb -L/usr/local/lib

echo 'export PATH="$PATH:$HOME/.local/bin"' >> ~/.profile
source ~/.profile
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

| Mode                                         | Purpose        | Mechanism                                                    | CPU Type Customizable |
| -------------------------------------------- | -------------- | ------------------------------------------------------------ | --------------------- |
| 🧠 **TCG (Tiny Code Generator)**              | Full emulation | Translates guest instructions into host instructions dynamically | ✅ Yes                 |
| ⚙️ **Hardware Virtualization (KVM/HVF/WHPX)** | Virtualization | Executes guest instructions directly on host CPU             | ❌ No                  |

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
