# 在 Proxmox VM 的 K3s 上部署本项目

## 结论与范围

本仓库不是 HTTP 微服务，而是由 `realtime_dw`、`rt_rule_mk` 两个 Flink 1.16.1 作业模块、一个 Spark 批处理模块和 Redis 演示模块组成。`deploy/k3s/` 提供 Flink 作业镜像 Dockerfile、Kubernetes 起步清单和镜像构建脚本；项目仍没有可直接部署的 fat JAR 或完整的 Helm Chart。

第一阶段建议把 **Flink 计算作业放入 K3s**，继续将 MySQL、Kafka、Redis、HBase、Doris 等状态服务放在已有的单机 Docker Compose/独立 VM 中。这样能先验证 K3s、镜像、作业调度和状态持久化；不要一次迁移所有大数据状态服务。项目源码中多处将服务地址写为 `doitedu`（Kafka `9092`、Redis `6379`、MySQL `3306`、ZooKeeper `2181`、Doris `8030`），迁移状态服务前必须先把这些地址改为配置项。

`batch_jobs` 当前使用 Spark 3.1.2，且作业中有本地运行模式配置；它不在本阶段的 K3s 部署范围。`redis_demo` 是连通性演示，也不应作为长期运行的工作负载。

## 当前访问状态

Proxmox 宿主机 `192.168.31.171:22` 已通过 `~/.ssh/id_ed25519_pve` 以 `root` 免密登录；`wdidada` 未被该宿主机授权。密钥指纹为 `SHA256:QzVeFC0dkyf5ta/e6rpdGsgHYq1TcY8v00c76cXmeKE`。日常运行应另建受限的运维账户，避免常态以 root 操作宿主机。

K3s 节点已在 2026-10-17 通过当日生成的 `~/.ssh/id_ed25519` 完成免密验证；`wdidada` 与 `root` 均可登录。昨天生成的 `id_rsa_proxmon` 不被这些节点接受。生产日常操作应使用受限的 `wdidada` 账户配合 `sudo`，不要常态使用 root。

| 节点 | IP | 角色 | 状态 |
| --- | --- | --- | --- |
| `k3s-master` | `192.168.31.77` | control-plane | Ready |
| `k3s-worker1` | `192.168.31.211` | worker | Ready |
| `k3s-worker2` | `192.168.31.251` | worker | Ready |
| `k3s-worker3` | `192.168.31.88` | worker | Ready |

集群运行 K3s `v1.35.4+k3s1`、CentOS Stream 9 和 containerd `2.2.3-k3s1`。`kube-system` 核心组件均在运行，现有工作负载位于 `galaxy-mall`、`middleware` 和 `shorturl` 命名空间；本项目必须使用独立的 `doit` 命名空间，不能修改已有资源。

### Proxmox VM 操作系统推荐

`Ubuntu 24.04 LTS` 与 `CentOS Stream 9` 本身都不算“重”；K3s 节点的主要资源消耗来自 kubelet、containerd、镜像缓存以及实际运行的 Pod，而不是没有桌面环境的基础系统。**不要安装 Desktop/GUI、开发工具全家桶或不需要的服务**；应使用 cloud image 或 Minimal/Server 安装，启用 `qemu-guest-agent`，并将容器镜像与数据目录放在 SSD 上。

| 场景 | 新建 VM 首选 | 可选方案 | 不建议作为默认选择 | 原因 |
| --- | --- | --- | --- | --- |
| K3s control-plane / worker | **Ubuntu Server 24.04 LTS Minimal（cloud image）** | Rocky Linux 9 Minimal | Desktop ISO、非 LTS Ubuntu | Ubuntu 资料与 K3s 示例多、驱动和 cloud-init 体验成熟；LTS 便于长期维护。 |
| 需要 RHEL 生态、SELinux/企业 RPM 规范 | **Rocky Linux 9 Minimal** | AlmaLinux 9、已验证的 CentOS Stream 9 | 为“绝对稳定”诉求新建 CentOS Stream 节点 | Rocky 9 是 RHEL 兼容的稳定发行版，生命周期长；CentOS Stream 是 RHEL 下一个小版本的持续交付分支，适合需要跟进 RHEL 生态或已标准化的团队。 |
| CI 构建机、短期实验 VM | Ubuntu Server 24.04 LTS Minimal | Debian 12 Minimal | 用生产 worker 兼任构建机 | 构建负载应与 K3s 工作负载隔离；本环境的 VM `107` 已承担该角色。 |

当前四个节点使用 CentOS Stream 9，且 K3s 已稳定运行，**不需要因为本建议而原地重装或迁移**。后续扩容时，同一集群优先保持同一发行版和同一大版本，避免混用导致内核、SELinux、包管理及故障处理路径不一致；若选择 Ubuntu，所有新节点统一为 Ubuntu 24.04 LTS Minimal。K3s 官方说明其可运行于大多数现代 Linux，基础最低资源只是起点，Flink 等工作负载仍须按实际容量规划资源。

安装后基线：静态 DHCP/固定 IP、唯一 hostname、NTP、SSH 密钥登录、最小化开放端口、定期安全更新；不要为了安装 K3s 直接关闭防火墙而不补齐 Pod/Service 网段和 API Server 的规则。

Proxmox 是单节点 PVE 8.4.0 宿主机（48 逻辑 CPU、约 64 GiB 内存）。核验时约 56.3 GiB 已使用、约 9.5 GiB 空闲；在调度新的 Flink JobManager/TaskManager 前必须设置 `requests` 和 `limits`，并先释放资源或扩容，不能按空闲 CPU 盲目部署。

| VMID | Proxmox 名称 | K3s 节点 | vCPU | 内存 | 系统盘 | 状态 |
| --- | --- | --- | ---: | ---: | ---: | --- |
| 100 | `wdidada` | `k3s-master` / `192.168.31.77` | 4 | 9048 MiB | 64 GiB | running |
| 101 | `VM 101` | `k3s-worker1` / `192.168.31.211` | 4 | 8048 MiB | 64 GiB | running |
| 103 | `work3` | `k3s-worker2` / `192.168.31.251` | 4 | 10240 MiB | 64 GiB | running |
| 104 | `work4` | `k3s-worker3` / `192.168.31.88` | 4 | 10240 MiB | 64 GiB | running |

### Flink 启动的硬性资源门禁

VM `107`（`ubuntu26-build`）是 **Ubuntu 26 构建机，不是 K3s 节点**。其配置为 12 vCPU、24576 MiB 内存和 1 TiB 磁盘；核验时处于 `running` 状态。它与 Flink 不能并行运行：**任何新的 FlinkDeployment 创建、恢复或扩容之前，VM 107 必须已经完全关机（`status: stopped`）**。这是为了给 K3s/Flink 释放宿主机资源，不能用降低 Flink 的资源配额来绕过该约束。

在 Proxmox 宿主机上执行以下步骤。先使用优雅关机；除非已确认 VM 无 I/O 且紧急处置需要，否则不要使用强制 `qm stop`：

```bash
# 1. 部署前检查。若输出不是 "status: stopped"，不得启动 Flink。
qm status 107

# 2. 仅在仍为 running 时执行一次优雅关机。
qm shutdown 107

# 3. 等待并复查；直到得到 "status: stopped" 才能继续。
qm status 107
```

在执行 `kubectl apply -f <flink-deployment.yaml>` 前，部署人员或自动化流程必须执行下面的门禁；返回非零即终止发布：

```bash
ssh -o BatchMode=yes -i ~/.ssh/id_ed25519_pve root@192.168.31.171 \
  'test "$(qm status 107)" = "status: stopped"'
```

Flink 作业停止、savepoint 已确认且资源回收完成后，是否重新启动构建机必须由运维人员显式决定：

```bash
# 仅在确实需要恢复 Ubuntu 构建机时执行；不应由 Flink 部署脚本自动执行。
qm start 107
```

不要把密码、私钥或 Kubeconfig 提交到本仓库。建议创建专用运维用户并安装其公钥：

```bash
# 在有密码或控制台权限时执行；替换为实际的非 root 运维账号。
ssh-copy-id -i ~/.ssh/id_ed25519.pub <operator>@192.168.31.171

# 本机验证必须是非交互式成功，才算免密可用。
ssh -o BatchMode=yes <operator>@192.168.31.171 'hostname; id; sudo -n true'
```

## 先核验 Proxmox 与 K3s

若 `192.168.31.171` 是 Proxmox 宿主机，在宿主机上执行：

```bash
pveversion -v
pvesh get /cluster/status
pvesh get /nodes
qm list                 # KVM 虚拟机
pct list                # LXC 容器
pvesh get /nodes/<node>/status
qm config <k3s-vmid>
```

确认四个 K3s 节点均运行在专用 Linux VM 中，而不是直接安装在 Proxmox 宿主机。记录每个节点的 VMID、vCPU、内存、磁盘、LAN IP、网关和 DNS；Flink 初始作业至少应预留一个 worker 的 4 vCPU、16 GiB 内存和 100 GiB SSD。若还要同时运行 Kafka、HBase、Doris 与 Flink，应按 [服务器部署规划](win11-and-server-deployment-plan.md) 的资源建议扩大配置或拆分 VM。

登录该 VM 后核验 K3s：

```bash
sudo systemctl is-active k3s
sudo k3s --version
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl get pods -A
sudo k3s kubectl get storageclass
sudo k3s kubectl get events -A --sort-by=.lastTimestamp
df -hT
```

当前默认存储类为 `local-path`（`rancher.io/local-path`），回收策略 `Delete`、绑定模式 `WaitForFirstConsumer`，且只提供 `ReadWriteOnce` 卷。它适合将单个起步作业固定在一个 worker 上，不适用于跨节点共享状态。Flink 的 checkpoint/savepoint 必须挂载到有备份策略的持久卷；要运行多个 TaskManager 或实现故障转移，应提供 RWX 存储或对象存储，而不是复用本文的本地 PVC。

## 推荐拓扑

```text
Proxmox 宿主机（192.168.31.171，PVE 8.4.0，单节点）
└── K3s 集群
    ├── k3s-master（192.168.31.77，control-plane）
    ├── k3s-worker1（192.168.31.211）
    ├── k3s-worker2（192.168.31.251）
    └── k3s-worker3（192.168.31.88）
        └── `doit` namespace
            ├── Flink Kubernetes Operator
            ├── Flink JobManager / TaskManager Pods
            ├── PersistentVolume：checkpoint、savepoint
            └── Cluster DNS：doitedu
                    │
                    └── 外部状态服务 VM / Docker Compose
                        Kafka、MySQL、Redis、HBase、Doris、ZooKeeper
```

外部状态服务必须只在 LAN/VPN 内可达。K3s VM 到这些服务的连通性、Kafka `advertised.listeners`、HBase/ZooKeeper 地址以及 MySQL 授权范围都必须先验收；仅让 Pod 能 ping 通并不表示 Flink 作业可以连接。

## K3s 基础准备

在管理端安装与目标集群版本匹配的 `kubectl` 和 Helm，并将 Kubeconfig 保存到本机受保护的位置（例如 `~/.kube/config`，权限 `0600`）。不要把它放入仓库。

```bash
kubectl cluster-info
kubectl get nodes -o wide
kubectl create namespace doit
kubectl create namespace flink-operator
```

为集群内作业保留 `doitedu` 这个 DNS 名称。若中间件仍在 K3s 之外，可在 `doit` 命名空间创建一个无头 Service 与 EndpointSlice；将 `192.168.31.171` 替换为**实际运行中间件的 LAN 地址**，而非假定的 Proxmox 地址：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: doitedu
  namespace: doit
spec:
  clusterIP: None
  ports:
    - { name: kafka, port: 9092, protocol: TCP }
    - { name: redis, port: 6379, protocol: TCP }
    - { name: mysql, port: 3306, protocol: TCP }
    - { name: zookeeper, port: 2181, protocol: TCP }
    - { name: doris-fe, port: 8030, protocol: TCP }
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: doitedu-external-ipv4
  namespace: doit
  labels:
    kubernetes.io/service-name: doitedu
addressType: IPv4
ports:
  - { name: kafka, protocol: TCP, port: 9092 }
  - { name: redis, protocol: TCP, port: 6379 }
  - { name: mysql, protocol: TCP, port: 3306 }
  - { name: zookeeper, protocol: TCP, port: 2181 }
  - { name: doris-fe, protocol: TCP, port: 8030 }
endpoints:
  - addresses: ["<middleware-lan-ip>"]
```

保存为临时受管清单后执行 `kubectl apply -f <file>`。验证 DNS 和端口时应使用临时诊断 Pod，而不是把凭据写入命令历史：

```bash
kubectl -n doit run dns-test --rm -it --restart=Never \
  --image=busybox:1.36 -- nslookup doitedu
```

## 原 Kafka/HBase/Doris 业务链路：内网地址替代 doitedu

`doitedu` 是部署主机的 DNS/hosts 名称，可以替换为实际 LAN IP 或 K3s Service 域名，但目标必须运行对应服务。Kafka broker 的 advertised.listeners、HBase 在 ZooKeeper 注册的 RegionServer RPC 地址，以及 Doris FE 返回的 BE 地址也必须可达，不能只检查最初的 bootstrap 端口。

本环境已为原入口 `EtlJob01_UserEventsLogCommonDim` 准备真实的 Kafka 3.9.1、HBase 2.4.18 和 Doris 2.1.8，并将地址拆为环境变量：Kafka/ZooKeeper 使用 worker1 `192.168.31.211`，Doris 使用 worker2 `192.168.31.251`，Flink 继续运行在 worker3。各服务使用固定节点 LAN 地址通告自身，独立的 `doit` 命名空间内保留 PVC。

**2026-10-17 已通过原业务链路验收**：同一唯一批次输入 5 条，Kafka 宽表输出 5 条，Doris 查询 5 行，三张 HBase 维度字段一致；作业 RUNNING，验收期间 checkpoint 成功数增加 7 次且无新增失败。

详细容量、no-AVX2 适配、部署清单、初始化和端到端验收脚本见 [原业务链路部署包](../deploy/k3s/business/README.md)。这套方案与下节的 datagen 独立验收作业共用同一个 FlinkDeployment，不能同时应用两份作业清单。

## 不使用 doitedu 的独立部署与访问方案

当没有 Kafka、HBase、Doris 时，使用本节的独立验收作业，无需为 `doitedu` 配置 DNS、hosts 或伪造中间件接口。入口为 `com.test.bigdata.rtdw.deployment.StandaloneEventsJob`，链路为 **datagen 每秒生成 2 条事件 → SQL 转换 → PVC 上的 JSON 文件**，每 5 秒执行 checkpoint。它验证真实的 Flink 流处理、持久化与 Web UI；**不代表原 ETL 的 Kafka/HBase/Doris 业务链路已经验收**，原入口保持不变。

### 已准备的运行环境

本方案复用本次在 macOS 12.7.6 上用自行编译的 Podman 4.9.5 构建的 Linux/amd64 镜像：

```text
docker.io/doit36/flink-jobs:mac12-podman495-20261017-1230
```

镜像 tar 的 SHA256 为 `5a119f7de116b9ae9f990fefee9ba0c7f3feeb584623c74cafe4727912e1dbf1`，已按 Mac → master → 三个 worker 的路径分发、逐台校验并导入 k3s containerd。master 到 worker 的免密账户是 `wdidada`；导入镜像和管理集群使用已授权的 root SSH。所有节点上的 tar 位于 `/home/wdidada/doit36-images/`。清单使用 `imagePullPolicy: Never`，新节点必须先导入相同镜像。

`deploy/k3s/flink-standalone.yaml` 复用 `doit/realtime-dw-etl-01` 和已绑定 worker3 的 `flink-state` PVC，以免同时启动两套 Flink 耗尽资源；应用它会以 stateless 方式切换当前作业。JobManager 为 1 CPU / 2 GiB，TaskManager 为 2 CPU / 4 GiB，并设置相同的资源上限。所有进程固定在 `k3s-worker3`，不支持跨节点共享这个 local-path PVC。

新增入口编译为小型 JAR，通过带内容摘要名称的 ConfigMap 挂载到 init 容器；init 将其与镜像中的 `flink-json-1.16.1.jar` 放入独立作业目录。Flink 运行时继续使用 Podman 构建的镜像，避免把原 ETL 的 Doris/HBase、重复 planner 等依赖带入此验收作业。ConfigMap 仅适用于这里的小 JAR；业务 JAR 应在后续正式镜像构建时打包。

### 复现部署

前提：已有 Flink Operator，镜像已在节点导入，VM 107 已停止。脚本会应用 `deploy/k3s/namespace.yaml`、`flink-rbac.yaml`、`flink-state-pvc.yaml`，准备命名空间、ServiceAccount/RBAC 和 PVC。worker3 必须存在并有足够资源；已有 `flink-state` PVC 的绑定节点也必须是 worker3。

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
bash deploy/k3s/deploy-standalone.sh
```

脚本先核验 VM 107 关机门禁，编译 `realtime_dw`，生成入口 JAR，以 SCP 传到 master，创建内容摘要版本的 ConfigMap，再应用 FlinkDeployment 和独立的 UI Service。SSH 主机默认是 `k3s-master`，可通过 `MASTER` 覆盖；PVE 地址和密钥可通过 `PVE`、`PVE_KEY` 覆盖。脚本不删除 PVC，不修改其他项目的命名空间。此环境没有公共镜像仓库，因此需要保留本地镜像缓存。

### 访问与验收

Web UI：<http://192.168.31.88:30816/>；REST：<http://192.168.31.88:30816/jobs/overview>。`deploy/k3s/flink-ui-nodeport.yaml` 将 30816 映射到 JobManager 的 8081。此入口只用于受信任 LAN；Flink UI 没有配置登录认证，不要将该端口转发到公网。

```bash
curl --noproxy '*' -fsS http://192.168.31.88:30816/jobs/overview
ssh k3s-master 'kubectl -n doit get pods -o wide'
# 将 JOB_ID 替换为 overview 返回的 jid。
curl --noproxy '*' -fsS http://192.168.31.88:30816/jobs/JOB_ID/checkpoints
ssh k3s-master 'kubectl -n doit exec deployment/realtime-dw-etl-01 -- sh -c "find /flink-data/standalone-events -name \"part-*\" -type f | head"'
```

验收条件是作业 `RUNNING`、checkpoint 的 `counts.completed` 持续增加，并且 `/flink-data/standalone-events` 中出现已提交的 `part-*` JSON 文件。每条输出包含 `event_id`、`event_type=page_view`、`channel=WEB` 和 `processed_at`。文件先滚动、再随 checkpoint 提交，因此启动后需要等待数十秒，不能只看 Pod Ready。

### 2026-10-17 实测结果

- Mac 直接访问 `http://192.168.31.88:30816/` 返回 HTTP 200，页面标题为 `Apache Flink Web Dashboard`，三个 JavaScript 资源均返回 HTTP 200。
- 作业 `standalone-events-validation`（ID `984df8f4df0d4f455f9f1feaa751441e`）为 `RUNNING`，任务 `running=1`；worker3 上 JobManager、TaskManager 均 `1/1 Ready`，重启次数为 0。
- 首次记录时 checkpoint `completed=8`，最新一次耗时 25 ms，元数据位于 `file:/flink-data/checkpoints/984df8f4df0d4f455f9f1feaa751441e/chk-8`。启动等待 TaskManager 时有 3 次未能触发的 checkpoint；任务进入 RUNNING 后连续成功。
- PVC 已产生多个已提交 `part-*` 文件，实际读取到以下记录：

```json
{"event_id":383159,"event_type":"page_view","channel":"WEB","processed_at":"2026-10-17 07:26:48.309Z"}
```

实际镜像中 `flink` 用户的 UID/GID 是 **9999**。清单的 `fsGroup` 与初始化目录的 owner 使用 9999；设置为 999 会造成 `AccessDeniedException`，使作业反复失败，即使 Pod 显示 Ready 也不能算验收通过。

JSON 文件滚动提交行为见 [Flink 1.16 文件系统连接器文档](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/filesystem/)。此作业持续产出文件；验收结束不再使用时，将 FlinkDeployment 的 `spec.job.state` 设置为 `suspended`，保留 PVC 供检查。恢复原 ETL 时必须重新准备真实中间件，不能直接把此验收作业当作业务替代品。

## 构建作业镜像

`k3s` 分支推送时，`Proxmox K3s Flink Image` GitHub Actions 会编译全部 Maven 模块，随后从 `target/classes` 创建作业 JAR 并显式复制运行时依赖。它使用 Podman 构建镜像并导出 tar，**不执行** Maven 的 `package` 阶段；当前 POM 没有 shade/assembly 插件，普通 Maven JAR 也不能假定包含第三方依赖。

推送默认分别在 `ubuntu-24.04` 和 `macos-15-intel` 上使用 Temurin 17 构建。工作流依次调用 `scripts/build-podman.sh` 下载并编译固定的 Podman 4.9.5，再调用 `scripts/build-image.sh`，显式用编译出的 Podman 构建和导出 docker-archive。手动点击 **Run workflow** 时可选择同时运行两个平台，或单选 Ubuntu `22.04`、`24.04`、`26.04`、Intel macOS 及 Temurin LTS JDK `17`、`21`、`25`；所选 JDK 同时用于 Maven 编译环境和最终镜像运行时。Artifact 命名为 `k3s-flink-image-<run-number>-<runner-os>-jdk<version>`，其中包含 `flink-jobs-<run-number>-<runner-os>-jdk<version>.tar`。镜像统一面向 `linux/amd64`；macOS runner 通过 QEMU Linux 虚拟机构建，Windows 不参与。当前 macOS 12.7 本地使用方法见 [scripts/README.md](../scripts/README.md)。

为让“编译整个项目”也能在高版本 JDK 下通过，`batch_jobs` 使用 Scala `2.12.21`（保持 Scala 2.12 二进制兼容），并使用新版 Scala Maven Plugin；不要将其降回仅支持 Java 17 的 Scala 2.12.12。

```bash
# 本地复现 CI 的镜像构建（不调用 Maven package）。
bash scripts/build-podman.sh
IMAGE_TAG=local bash scripts/build-image.sh

# 将 CI Artifact 下载到可能运行 Flink Pod 的每个 K3s 节点后导入。
sudo k3s ctr images import flink-jobs-<run-number>.tar
```

镜像保留官方 `apache/flink:1.16.1-scala_2.12-java11` 的 Flink 运行时和入口脚本，并以多阶段构建加入所选的 Temurin LTS JRE。项目作业目标字节码保持 Java 17，选择 JDK 21 或 25 用于兼容性验证时不会自动提高项目的 `maven.compiler.target`。应用 JAR 和运行时依赖位于 `/opt/flink/usrlib/`；不要使用 `latest` 标签。部署前确认 JAR 中只有一个明确的入口类，例如：

```text
com.test.bigdata.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim
```

Flink 基础镜像、运行时 JDK 和连接器版本必须在测试环境一致。项目使用 Flink 1.16.1，不能将作业 JAR 随意部署到其他大版本的 Flink 运行时。

## 安装并使用 Flink Kubernetes Operator

使用稳定版 Operator，并在变更前阅读该版本的兼容矩阵与 Helm values。Operator 默认启用 webhook 时，需要预先安装 cert-manager；官方文档也说明可在不使用 webhook 的学习环境显式关闭它。不要将 `<OPERATOR_VERSION>` 或镜像 tag 留为 `latest`。

```bash
# 使用与目标 K3s 兼容、已验证的稳定 Operator 版本。
helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-<OPERATOR_VERSION>/
helm install flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace

kubectl -n flink-operator get pods
kubectl get crd | grep flink.apache.org
```

以下 `FlinkDeployment` 是单作业起步模板。它将 JobManager/TaskManager 固定到 `k3s-worker1`，以便安全使用默认的单节点 `local-path` PVC；将镜像、JAR 文件名、入口类和 PVC 名称替换为已验证的值。要启用跨 worker 调度或多个 TaskManager，先部署 RWX/对象存储状态后端，再移除该 `nodeSelector`。

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: flink-state
  namespace: doit
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  resources:
    requests:
      storage: 50Gi
---
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: realtime-dw-etl-01
  namespace: doit
spec:
  image: <registry>/doit36-realtime:<immutable-tag>
  flinkVersion: v1_16
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.checkpoints.dir: file:///flink-data/checkpoints
    state.savepoints.dir: file:///flink-data/savepoints
  podTemplate:
    spec:
      nodeSelector:
        kubernetes.io/hostname: k3s-worker1
      containers:
        - name: flink-main-container
          volumeMounts:
            - name: flink-state
              mountPath: /flink-data
      volumes:
        - name: flink-state
          persistentVolumeClaim:
            claimName: flink-state
  jobManager:
    resource:
      cpu: 1
      memory: "2048m"
  taskManager:
    resource:
      cpu: 2
      memory: "4096m"
  job:
    jarURI: local:///opt/flink/usrlib/realtime_dw-1.0-SNAPSHOT.jar
    entryClass: com.test.bigdata.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim
    parallelism: 1
    upgradeMode: stateless
    state: running
```

先以 `parallelism: 1` 验证一条链路。确认 JobManager、TaskManager、Kafka、MySQL/HBase/Doris 的连接和 checkpoint 均正常后，再增大并行度与资源配额。作业启动时如遇 Java 17 模块访问错误，按现有服务器部署规划补充项目要求的 `--add-opens` JVM 参数。

## 发布、验证与回滚

```bash
kubectl apply -f <flink-deployment.yaml>
kubectl -n doit get flinkdeployments
kubectl -n doit get pods -w
kubectl -n doit logs deploy/flink-kubernetes-operator -c flink-kubernetes-operator

# 查看 JobManager 服务与作业日志；名称以实际生成的资源为准。
kubectl -n doit get svc
kubectl -n doit logs <jobmanager-pod> --all-containers
```

验收前的第一个门禁是 VM `107` 必须为 `stopped`。随后确认 Pod 均为 Ready、Flink 作业为 `RUNNING`、Kafka consumer lag 不持续增长、数据可写入目标 MySQL/HBase/Doris、Redis 示例能连通、checkpoint 连续完成且 savepoint 能恢复。版本升级前先创建 savepoint；回滚时恢复上一个不可变镜像 tag 和相应 savepoint，不能只重启 Pod 后假定状态仍在。

## 后续改造清单

1. 为每个要长期运行的 Flink 作业补充独立的可配置入口，不再依赖 `doitedu`、Windows 盘符或硬编码密码。
2. 新增可复现的 Dockerfile、依赖打包策略及 GitHub Actions 镜像构建/推送流程。
3. 将本文中的 Service、EndpointSlice、PVC、FlinkDeployment 等清单保存到版本控制的 `deploy/k8s/`，并使用环境专属 overlays 管理 IP 和资源配额。
4. 为 Kafka、HBase、Doris 等状态组件设计备份、恢复、监控与多节点方案后，再评估迁入 Kubernetes；不要将单节点 K3s 学习配置当成生产高可用方案。

## 参考资料

- [Apache Flink Kubernetes Operator 安装文档](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/deployment/helm/installation/)
- [Apache Flink Kubernetes Operator 快速开始](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/try-flink-kubernetes-operator/quick-start/)
