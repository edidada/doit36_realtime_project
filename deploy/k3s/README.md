# K3s 部署包模板

`Proxmox K3s Flink Image` 工作流会先编译所有 Maven 模块，再将两个 Flink
作业及其运行时依赖写入镜像并通过 Podman 导出 tar；它不会在 GitHub Actions 中连接或修改
内网 K3s 集群，也不会执行 Maven 的 `package` 阶段。

工作流依次调用 `scripts/build-podman.sh` 和 `scripts/build-image.sh`，
从源码编译固定 Podman 4.9.5，再用该二进制构建并导出镜像。
本机 macOS 12.7 和 Linux 的依赖、命令及配置见 [脚本说明](../../scripts/README.md)。

部署前必须遵守 [K3s 部署文档](../../docs/k3s-deployment.md) 中的资源门禁：Proxmox VM `107`（`ubuntu26-build`）必须为 `stopped`。然后在目标集群执行：

```bash
kubectl apply -f namespace.yaml
kubectl -n doit apply -f flink-state-pvc.yaml
```

`flink-deployment.yaml.tpl` 是模板，必须将 `<IMAGE_TAG>` 替换为 GitHub Actions 的完整镜像 tag。推送默认分别在 `ubuntu-24.04` 和 `macos-15-intel` 构建，tag 为 `run-number-<runner-os>-jdk17`；手动运行可选择单个平台或同时运行，以及 Temurin LTS JDK `17`、`21`、`25`。`k3s.yml` 会构建 `doit36/flink-jobs:<IMAGE_TAG>`，并导出 `deploy/k3s/output/flink-jobs-<IMAGE_TAG>.tar`。将该 tar 复制到可能运行 Flink Pod 的节点后执行：

```bash
sudo k3s ctr images import flink-jobs-<IMAGE_TAG>.tar
sudo k3s ctr images list | grep 'doit36/flink-jobs'
```

该模板默认运行 `realtime_dw` 作业；改为规则引擎时，将 `jarURI` 改为 `local:///opt/flink/usrlib/rt_rule_mk/rt_rule_mk-job.jar` 并指定对应入口类。它假设 Flink Kubernetes Operator 已在集群中安装，并将 JobManager/TaskManager 固定在 `k3s-worker1`，以兼容当前 `local-path` 的 RWO 存储。

不要将本机 SSH 私钥、Kubeconfig、数据库密码或中间件地址写入此目录。

## 原业务链路使用内网 IP

真实 Kafka/HBase/Doris 部署、原 ETL 配置和双输出验收见
[业务链路部署包](business/README.md)。当前环境使用该方案运行原业务入口；
下节的独立作业仅用于基础设施诊断，重新部署它会替换当前业务作业。

## 不依赖 doitedu 的验收作业

已有本次 Podman 镜像缓存时，在仓库根目录执行：

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
bash deploy/k3s/deploy-standalone.sh
```

这会把 `doit/realtime-dw-etl-01` 切换为独立的 datagen → SQL → PVC JSON 验收作业，
不连接 Kafka/HBase/Doris。它复用 worker3 上的状态卷，保留原 ETL 源码。
访问 <http://192.168.31.88:30816/>，并检查作业 RUNNING、checkpoint 和已提交文件；
完整前提、复现方式和验收记录见 [不使用 doitedu 的方案](../../docs/k3s-deployment.md#不使用-doitedu-的独立部署与访问方案)。
