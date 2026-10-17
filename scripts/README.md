# 从源码编译 Podman，再构建 K3s 镜像

在仓库根目录执行：

```bash
bash scripts/build-podman.sh
bash scripts/build-image.sh
```

第一个脚本固定下载 **Podman v4.9.5** 源码，通过已知 Go 模块摘要校验，
并使用 **Go 1.24.6** 编译。已有相同 Go 版本就复用，否则下载对应平台的
Go 压缩包并校验固定 SHA256。所有下载、缓存和二进制保存在
`.artifacts/podman-build/`，不会替换系统 Go 或 Podman。

第二个脚本显式调用 `.artifacts/podman-build/bin/podman`，先执行
`mvn -DskipTests clean compile`，再复用 `deploy/k3s/build-k3s-image.sh`
收集两个作业的类和依赖、构建镜像，最后导出 **docker-archive** 格式 tar。
默认目标为 `linux/amd64`，与当前 Proxmox K3s 节点一致。
Apple Silicon 构建该目标时，Linux 虚拟机需配置 amd64 指令模拟；
为 ARM 集群构建时可设置 `IMAGE_PLATFORM=linux/arm64`。

## macOS（含本机 12.7）

支持 Intel 和 Apple Silicon，使用系统 Bash 3.2 可执行的语法。
macOS 编译的是远程客户端，容器构建仍在 Linux 虚拟机中执行。
脚本同时从固定 v0.7.2 源码编译 QEMU 网络辅助程序 `gvproxy`。
本机已有可用的 QEMU Podman machine 时，直接运行上面两条命令即可。
需要 JDK 17 或以上、Maven、Python 3、curl、make；本机可先设置：

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

新环境需要与该 macOS 兼容的 QEMU；已有旧系统的 QEMU 可继续使用，
不需要安装最新版 Podman。现代 macOS 的 CI runner 使用 `brew install qemu`。
允许脚本创建、启动名为 `doit36-build` 的独立虚拟机时执行：

```bash
INIT_PODMAN_MACHINE=true bash scripts/build-image.sh
```

该虚拟机默认 2 CPU、4 GiB 内存、30 GiB 磁盘，会一直保留，
可用编译出的 `podman machine stop doit36-build` 停止。
`PODMAN_MACHINE_IMAGE` 可指定已有的 qcow2.xz 文件或镜像 URL；
不指定时使用 Podman 4.9.5 的默认 Fedora CoreOS 镜像。
`PODMAN_MACHINE_NAME` 可更改虚拟机名。自动创建虚拟机时仅为本次进程选择
对应连接；未启用自动创建时沿用当前 Podman 连接。

## Linux

Linux 编译完整的本地引擎，需要 C 编译器、pkg-config 和 libseccomp 开发包，
运行时需要 conmon、OCI runtime、网络工具以及 rootless UID/GID 映射。
Ubuntu 可先安装（发行版 Podman 包仅用来提供运行时依赖与配置）：

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libseccomp-dev podman
bash scripts/build-podman.sh
bash scripts/build-image.sh
```

Ubuntu 22.04 的发行版运行时使用 CNI。CI 为该版本提供独立的
`[network] network_backend = "cni"` 配置；本地使用时也需要匹配已安装的
CNI 或 Netavark 网络后端。其他 Linux 发行版需安装等价依赖。

## 配置和 CI

两个脚本通过 `PODMAN_BUILD_ROOT` 共用构建目录。下载受限时可设置
`GOPROXY=https://goproxy.cn,direct` 或 `GO_DOWNLOAD_BASE` Go 下载镜像站；
固定校验摘要不随镜像站改变。

镜像脚本支持现有的 `IMAGE_NAMESPACE`、`IMAGE_NAME`、`IMAGE_TAG`、
`RUNTIME_JDK`、`OUTPUT_DIR`，以及 `IMAGE_PLATFORM`。
基础镜像默认使用完整的 Docker Hub 地址，支持通过 `TEMURIN_IMAGE` 和
`FLINK_IMAGE` 指定镜像站地址或已缓存镜像，避免 short-name 解析提示。
例如：

```bash
IMAGE_TAG=podman495 RUNTIME_JDK=17 bash scripts/build-image.sh
# 输出 deploy/k3s/output/flink-jobs-podman495.tar
sudo k3s ctr images import deploy/k3s/output/flink-jobs-podman495.tar
```

已有完整编译结果、仅重做镜像时可设置 `SKIP_MAVEN_COMPILE=true`。
两种脚本均排除 Windows。K3s 工作流在 push 时默认分别运行
`ubuntu-24.04` 和 `macos-15-intel`，两者依次调用这两个脚本，上传独立 tar。
手动运行可选择两个平台一起跑，或单选 Ubuntu 22.04/24.04/26.04、Intel macOS，
并选择 JDK 17/21/25。macOS 使用 Intel runner，避免 ARM runner 的嵌套虚拟化限制；
CI 固定使用 Fedora CoreOS `39.20240322.3.0` 的 QEMU 镜像，并在结束后停止虚拟机。
Linux CI 在临时 runner 上允许自编译二进制创建 rootless user namespace。
