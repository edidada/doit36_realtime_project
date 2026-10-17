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

## 构建作业镜像

`k3s` 分支推送时，`Proxmox K3s Flink Image` GitHub Actions 会编译全部 Maven 模块，随后从 `target/classes` 创建作业 JAR 并显式复制运行时依赖。它**不执行** Maven 的 `package` 阶段；当前 POM 没有 shade/assembly 插件，普通 Maven JAR 也不能假定包含第三方依赖。工作流会导出 `deploy/k3s/output/flink-jobs-<run-number>.tar` 作为 Artifact。

```bash
# 本地复现 CI 的镜像构建（不调用 Maven package）。
mvn -DskipTests clean compile
IMAGE_TAG=local SAVE_TAR=true bash deploy/k3s/build-k3s-image.sh

# 将 CI Artifact 下载到可能运行 Flink Pod 的每个 K3s 节点后导入。
sudo k3s ctr images import flink-jobs-<run-number>.tar
```

镜像保留官方 `apache/flink:1.16.1-scala_2.12-java11` 的 Flink 运行时和入口脚本，并安装校验过的 Temurin Java 17 JRE，因为项目作业编译目标为 Java 17。应用 JAR 和运行时依赖位于 `/opt/flink/usrlib/`；不要使用 `latest` 标签。部署前确认 JAR 中只有一个明确的入口类，例如：

```text
cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim
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
    entryClass: cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim
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
