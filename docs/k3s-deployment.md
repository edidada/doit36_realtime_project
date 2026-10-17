# 在 Proxmox VM 的 K3s 上部署本项目

## 结论与范围

本仓库不是 HTTP 微服务，而是由 `realtime_dw`、`rt_rule_mk` 两个 Flink 1.16.1 作业模块、一个 Spark 批处理模块和 Redis 演示模块组成。仓库当前没有 Dockerfile、Kubernetes 清单、Helm Chart 或可直接部署的 fat JAR。

第一阶段建议把 **Flink 计算作业放入 K3s**，继续将 MySQL、Kafka、Redis、HBase、Doris 等状态服务放在已有的单机 Docker Compose/独立 VM 中。这样能先验证 K3s、镜像、作业调度和状态持久化；不要一次迁移所有大数据状态服务。项目源码中多处将服务地址写为 `doitedu`（Kafka `9092`、Redis `6379`、MySQL `3306`、ZooKeeper `2181`、Doris `8030`），迁移状态服务前必须先把这些地址改为配置项。

`batch_jobs` 当前使用 Spark 3.1.2，且作业中有本地运行模式配置；它不在本阶段的 K3s 部署范围。`redis_demo` 是连通性演示，也不应作为长期运行的工作负载。

## 当前访问状态

`192.168.31.171:22` 的 Proxmox 宿主机仍无法使用默认密钥或 `id_rsa_proxmon` 以 `wdidada`/`root` 登录，因此 Proxmox 层的 VMID、资源和网络配置尚未核验。

K3s 节点已在 2026-10-17 通过当日生成的 `~/.ssh/id_ed25519` 完成免密验证；`wdidada` 与 `root` 均可登录。昨天生成的 `id_rsa_proxmon` 不被这些节点接受。生产日常操作应使用受限的 `wdidada` 账户配合 `sudo`，不要常态使用 root。

| 节点 | IP | 角色 | 状态 |
| --- | --- | --- | --- |
| `k3s-master` | `192.168.31.77` | control-plane | Ready |
| `k3s-worker1` | `192.168.31.211` | worker | Ready |
| `k3s-worker2` | `192.168.31.251` | worker | Ready |
| `k3s-worker3` | `192.168.31.88` | worker | Ready |

集群运行 K3s `v1.35.4+k3s1`、CentOS Stream 9 和 containerd `2.2.3-k3s1`。`kube-system` 核心组件均在运行，现有工作负载位于 `galaxy-mall`、`middleware` 和 `shorturl` 命名空间；本项目必须使用独立的 `doit` 命名空间，不能修改已有资源。

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
Proxmox 宿主机（192.168.31.171，待核验）
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

先在 CI 或专用构建机打包。当前 POM 没有 shade/assembly 插件，因此 `mvn package` 生成的是普通 JAR，不能假定它已经包含所有第三方依赖。镜像构建必须显式复制运行时依赖，或先为对应模块补充并验证可复现的 fat-JAR 构建。

```bash
# 先选择一个作业模块和一个入口类，不要同时运行所有 Demo。
mvn -pl realtime_dw -am clean package -DskipTests
mvn -pl realtime_dw dependency:copy-dependencies \
  -DincludeScope=runtime \
  -DoutputDirectory=realtime_dw/target/dependency
```

镜像应基于经过验证的 Flink 1.16.1 / Java 17 基础镜像，将应用 JAR 和所需依赖放到 `/opt/flink/usrlib/`；不要使用 `latest` 标签。镜像推送到 K3s 节点可拉取的私有仓库，并使用不可变 tag（例如 Git commit SHA）。部署前确认 JAR 中只有一个明确的入口类，例如：

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

验收至少包括：Pod 均为 Ready、Flink 作业为 `RUNNING`、Kafka consumer lag 不持续增长、数据可写入目标 MySQL/HBase/Doris、Redis 示例能连通、checkpoint 连续完成且 savepoint 能恢复。版本升级前先创建 savepoint；回滚时恢复上一个不可变镜像 tag 和相应 savepoint，不能只重启 Pod 后假定状态仍在。

## 后续改造清单

1. 为每个要长期运行的 Flink 作业补充独立的可配置入口，不再依赖 `doitedu`、Windows 盘符或硬编码密码。
2. 新增可复现的 Dockerfile、依赖打包策略及 GitHub Actions 镜像构建/推送流程。
3. 将本文中的 Service、EndpointSlice、PVC、FlinkDeployment 等清单保存到版本控制的 `deploy/k8s/`，并使用环境专属 overlays 管理 IP 和资源配额。
4. 为 Kafka、HBase、Doris 等状态组件设计备份、恢复、监控与多节点方案后，再评估迁入 Kubernetes；不要将单节点 K3s 学习配置当成生产高可用方案。

## 参考资料

- [Apache Flink Kubernetes Operator 安装文档](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/deployment/helm/installation/)
- [Apache Flink Kubernetes Operator 快速开始](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/try-flink-kubernetes-operator/quick-start/)
