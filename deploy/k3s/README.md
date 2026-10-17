# K3s 部署包模板

本目录随 `Build K3s Deployment Bundle` 工作流打包，不会在 GitHub Actions 中连接或修改 K3s 集群。

部署前必须遵守 [K3s 部署文档](../../docs/k3s-deployment.md) 中的资源门禁：Proxmox VM `107`（`ubuntu26-build`）必须为 `stopped`。然后在目标集群执行：

```bash
kubectl apply -f namespace.yaml
kubectl -n doit apply -f flink-state-pvc.yaml
```

`flink-deployment.yaml.tpl` 是模板，必须将 `<IMAGE_TAG>` 替换为 GitHub Actions 的运行编号。`k3s.yml` 会构建 `doit36/flink-jobs:<IMAGE_TAG>`，并导出 `deploy/k3s/output/flink-jobs-<IMAGE_TAG>.tar`。将该 tar 复制到可能运行 Flink Pod 的节点后执行：

```bash
sudo k3s ctr images import flink-jobs-<IMAGE_TAG>.tar
sudo k3s ctr images list | grep 'doit36/flink-jobs'
```

该模板默认运行 `realtime_dw` 作业；改为规则引擎时，将 `jarURI` 改为 `local:///opt/flink/usrlib/rt_rule_mk/rt_rule_mk-job.jar` 并指定对应入口类。它假设 Flink Kubernetes Operator 已在集群中安装，并将 JobManager/TaskManager 固定在 `k3s-worker1`，以兼容当前 `local-path` 的 RWO 存储。

不要将本机 SSH 私钥、Kubeconfig、数据库密码或中间件地址写入此目录。
