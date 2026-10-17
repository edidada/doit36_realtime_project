# K3s 三项后续改造的部署与测试记录

## 范围与结论

本文把 `k3s` 分支 `docs/k3s-deployment.md` 的“后续改造清单”中前
三项拆成可复现的部署和验收步骤：

1. 为长期运行的 Flink 作业提供独立、可配置的入口；
2. 用可复现的 Podman 构建、导出镜像，并由 GitHub Actions 运行；
3. 将 K3s 部署清单纳入版本控制，并为不同环境保留明确的配置边界。

部署实现和验收脚本位于 `k3s` 分支的 `deploy/k3s/`，而不是当前
`master`。因此，本文提交到 `master` 是为了保留操作记录；实际执行下列
部署命令前，必须先检出包含这些文件的 `k3s` 提交（记录时为
`9af5bb4`）或将对应文件以受审查的方式合入目标分支。不要在只有本文的
`master` 上直接执行 `deploy/k3s/...` 命令。

| 改造项 | 已验证内容 | 不能据此推导的结论 |
| --- | --- | --- |
| 独立入口 | datagen → SQL → PVC 文件链路可运行、可 checkpoint | 原 Kafka/HBase/Doris 业务 ETL 已验收 |
| 可复现镜像 | 全模块 `compile`、Podman 构建、tar 导出及节点导入流程 | 已推送到公共镜像仓库 |
| 版本化清单 | namespace、RBAC、PVC、FlinkDeployment、UI、业务中间件清单均有源码 | 已具备生产多活、备份和灾备能力 |

## 共用前提与安全门禁

记录中的集群为一主三工作节点：master `192.168.31.77`，worker 分别为
`192.168.31.211`、`192.168.31.251`、`192.168.31.88`。所有 `kubectl`
命令在 master 执行，业务命名空间为 `doit`。

开始任何 Flink 部署前，必须确认 Proxmox 上 VM `107`（`ubuntu26-build`）
为关机状态。它是资源门禁，不应为了部署而启动：

```bash
ssh -o BatchMode=yes -i ~/.ssh/id_ed25519_pve root@192.168.31.171 \
  'test "$(qm status 107)" = "status: stopped"'
```

另外确认 Operator、节点和 PVC 正常。当前 `local-path` PVC 是 `ReadWriteOnce`
存储，工作负载和该 PVC 必须固定在同一个 worker，不能把它当作跨节点共享盘。

```bash
ssh root@192.168.31.77 'kubectl get nodes; kubectl -n doit get pvc,pods -o wide'
ssh root@192.168.31.77 'kubectl -n flink-operator get pods'
```

## 1. 独立、可配置的 Flink 作业入口

### 实现方式

验证入口为
`cn.doitedu.rtdw.deployment.StandaloneEventsJob`。它不连接 `doitedu`、
Kafka、HBase 或 Doris，而是每秒生成两条测试事件，经过 Flink SQL 后写入
PVC 上的 JSON 文件，并每 5 秒触发 checkpoint。这样可以把“集群、Flink
运行时、状态卷是否可用”的基础设施验收，与真实业务中间件的可达性分开。

`deploy/k3s/deploy-standalone.sh` 的流程是：

1. 两次检查 VM 107 关机门禁（编译前和创建/恢复 Flink 负载前）；
2. 用 JDK 17 编译 `realtime_dw`，从 `target/classes` 生成小型入口 JAR；
3. 以 JAR SHA-256 前缀创建不可变名称的 ConfigMap；
4. 应用 namespace、RBAC、`flink-state` PVC、FlinkDeployment 和 UI Service；
5. 由 init 容器将入口 JAR 与镜像中已有的 `flink-json-1.16.1.jar` 放入独立
   作业目录。

`flink-standalone.yaml` 将 JobManager、TaskManager 固定在 `k3s-worker3`，
挂载 `flink-state` 到 `/flink-data`，并使用 `fsGroup: 9999`。镜像内 `flink`
用户的 UID/GID 为 9999；使用 999 会导致 `AccessDeniedException`，即使 Pod
表面 Ready 也不能算通过。

### 部署与验证

在含有部署包的工作树中执行：

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)  # macOS 示例
bash deploy/k3s/deploy-standalone.sh

# 集群状态和作业状态
ssh root@192.168.31.77 'kubectl -n doit get pods -o wide'
ssh root@192.168.31.77 'kubectl -n doit get flinkdeployment realtime-dw-etl-01'

# NodePort 仅限受信任 LAN；JOB_ID 取 overview 返回的 jid。
curl --noproxy '*' -fsS http://192.168.31.88:30816/jobs/overview
curl --noproxy '*' -fsS http://192.168.31.88:30816/jobs/JOB_ID/checkpoints
ssh root@192.168.31.77 \
  'kubectl -n doit exec deployment/realtime-dw-etl-01 -- sh -c "find /flink-data/standalone-events -name '\''part-*'\'' -type f | head"'
```

验收条件不是只看 Pod Ready，而是同时满足：Flink 作业为 `RUNNING`、
`counts.completed` 持续增加、`/flink-data/standalone-events` 出现已提交的
`part-*` JSON 文件。文件会在滚动后随 checkpoint 提交，启动后应至少等待数十秒。

2026-10-17 的实测记录为：Web UI `http://192.168.31.88:30816/` 返回 HTTP
200；作业 `standalone-events-validation` 处于 `RUNNING`；JobManager 和
TaskManager 均为 `1/1 Ready`、重启次数 0；已完成 checkpoint 从 8 次继续增长，
并在 PVC 中读到包含 `event_id`、`event_type=page_view`、`channel=WEB` 和
`processed_at` 的 JSON 记录。

此入口仅是基础设施诊断作业。恢复原 ETL 前必须重新确认 Kafka、HBase、Doris
和各自的 advertised/注册地址可达；不能将该结果写成“业务链路已通过”。不再使用
诊断作业时，将其 `spec.job.state` 改为 `suspended`，保留 PVC 供检查；若切换
有状态业务版本，应先创建 savepoint，再恢复上一不可变镜像 tag 和对应 savepoint。

## 2. 可复现的 Podman 镜像构建与 GitHub Actions

### 构建边界

`k3s` 分支的 `.github/workflows/k3s.yml`（工作流名 `Proxmox K3s Flink Image`）
会在推送涉及源码、POM、部署文件、构建脚本或 K3s 文档时运行，也支持手动选择：

- Linux：Ubuntu 22.04、24.04 或 26.04；
- Intel macOS：通过 QEMU 构建 `linux/amd64` 镜像；
- Temurin LTS JDK 17、21 或 25；默认自动验证 Ubuntu 24.04 与 Intel macOS。

流程先以 `mvn --batch-mode --no-transfer-progress --show-version -DskipTests clean compile`
编译所有 Maven 模块，再通过 `scripts/build-podman.sh` 编译固定的 Podman 4.9.5，
最后运行 `scripts/build-image.sh` 和 `deploy/k3s/build-k3s-image.sh`。这不是 Maven
`package`；当前 POM 没有可依赖的 shade/assembly fat-jar 流程，作业 JAR 与明确
复制的运行时依赖由镜像构建步骤处理。

每个 job 会列出 `deploy/k3s/output/` 的 tar 文件，并上传名为
`k3s-flink-image-<run-number>-<runner-os>-jdk<version>` 的 GitHub Artifact。
镜像 tag 为 `doit36/flink-jobs:<run-number>-<runner-os>-jdk<version>`，不使用
`latest`。Artifact 只包含镜像 tar，不会连接或修改内网 K3s 集群。

### 导入、部署前检查与验收

将同一 tar 分发到所有可能调度 Flink Pod 的节点；本环境不依赖公共镜像仓库，
清单使用 `imagePullPolicy: Never`，因此每个候选节点都必须导入相同镜像。

```bash
# 在每个可能运行 JobManager/TaskManager 的 K3s 节点执行。
sudo k3s ctr images import flink-jobs-<tag>.tar
sudo k3s ctr images list | grep 'doit36/flink-jobs'

# 在构建工作树中确认导出的结果，而不是猜测文件名。
ls -lh deploy/k3s/output/
```

导入后再应用对应的 FlinkDeployment，检查实际 Pod 所在节点和镜像 ID：

```bash
ssh root@192.168.31.77 'kubectl -n doit get pods -o wide'
ssh root@192.168.31.77 \
  'kubectl -n doit get pods -o jsonpath="{range .items[*]}{.metadata.name}{\"\\t\"}{.spec.nodeName}{\"\\t\"}{.status.containerStatuses[*].imageID}{\"\\n\"}{end}"'
```

验收以四项为准：CI 的 `compile`、Podman build、`ls -lh` 发现 tar、所有实际调度
节点的 containerd 都能列出相同 tag。随后仍须执行第 1 节或真实业务链路的运行时验收；
“tar 成功上传”为构建通过，不等于作业可运行。

## 3. 版本化部署清单与环境边界

原清单建议使用 `deploy/k8s/`；实际实现选择了更具体的 `deploy/k3s/`。目录中应与
源码一起评审和变更的文件包括：

| 文件 | 作用 |
| --- | --- |
| `namespace.yaml`、`flink-rbac.yaml` | `doit` 命名空间及 Operator 所需权限 |
| `flink-state-pvc.yaml` | checkpoint、savepoint 与验收输出的 RWO 状态卷 |
| `flink-deployment.yaml.tpl` | 正式作业镜像 tag、入口类、资源和状态目录模板 |
| `flink-standalone.yaml`、`flink-ui-nodeport.yaml` | 独立验收作业和只限内网的 Web UI |
| `business/middleware.yaml`、`business/flink-business.yaml` | Kafka/HBase/Doris 原业务链路的中间件与 Flink 清单 |
| `business/prepare-middleware.sh`、`business/verify-business.py` | 中间件准备和端到端验收脚本 |

应用前先渲染或检查 YAML，明确填入不可变 image tag、节点选择和资源配额；不得把 SSH
私钥、kubeconfig、密码或中间件地址写进 Git。位于集群外的中间件若要继续使用
`doitedu` 名称，应通过 Service + EndpointSlice 指向**实际** LAN 地址，且必须逐项验证
Kafka advertised listener、HBase RegionServer/ZooKeeper 地址以及 Doris FE/BE 返回地址。

```bash
# 模板中的 <IMAGE_TAG> 必须替换为 CI Artifact 对应的完整 tag。
sed 's#<IMAGE_TAG>#<immutable-image-tag>#g' deploy/k3s/flink-deployment.yaml.tpl \
  > /tmp/flink-deployment.yaml
kubectl apply --dry-run=client -f /tmp/flink-deployment.yaml

# 先创建基础资源，再部署作业；实际 namespace 以 YAML 为准。
kubectl apply -f deploy/k3s/namespace.yaml
kubectl -n doit apply -f deploy/k3s/flink-state-pvc.yaml
kubectl -n doit get pvc,pods,svc
```

原业务链路的 2026-10-17 验收记录为：同一唯一批次输入 5 条，Kafka 宽表输出 5 条，
Doris 查询 5 行，三张 HBase 维度字段一致；Flink 作业为 `RUNNING`，验收期间成功
checkpoint 增加 7 次且没有新增失败。这是该次环境和数据集的结果，重建环境后应重新运行
`business/verify-business.py`，不能只复用这条记录。

目前“环境专属 overlays”仍是明确的后续工作：至少将节点名、外部中间件 LAN IP、
NodePort、镜像 tag、CPU/内存和存储类从基础清单中分离；可用 Kustomize overlays 或
Helm values 实现。任何 overlay 在推广前均应依次通过 `kubectl apply --dry-run=client`、
单节点/单并行度部署、checkpoint 与数据端到端检查。Kafka、HBase、Doris 的备份、恢复、
监控和多节点方案不属于这三项的完成范围，在具备这些能力前，本 K3s 集群只应作为学习和
验证环境，而非高可用生产环境。

## 执行后记录模板

每次测试请追加或保存以下事实，便于问题可重现：

```text
源码提交：
镜像完整 tag 与 tar SHA-256：
导入节点及 k3s ctr image ID：
VM 107 门禁结果：
FlinkDeployment generation / job 状态：
JobManager、TaskManager 节点与重启次数：
checkpoint completed / failed：
输入、输出与目标存储核对结果：
回滚的镜像 tag / savepoint：
```
