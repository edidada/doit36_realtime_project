# K3s 部署包模板

本目录随 `Build K3s Deployment Bundle` 工作流打包，不会在 GitHub Actions 中连接或修改 K3s 集群。

部署前必须遵守 [K3s 部署文档](../../docs/k3s-deployment.md) 中的资源门禁：Proxmox VM `107`（`ubuntu26-build`）必须为 `stopped`。然后在目标集群执行：

```bash
kubectl apply -f namespace.yaml
kubectl -n doit apply -f flink-state-pvc.yaml
```

`flink-deployment.yaml.tpl` 是模板，必须将 `<REGISTRY>`, `<IMAGE_TAG>` 和 `<JAR_FILE>` 替换为经过验证的值后，才能应用。它假设 Flink Kubernetes Operator 已在集群中安装，并将 JobManager/TaskManager 固定在 `k3s-worker1`，以兼容当前 `local-path` 的 RWO 存储。

不要将本机 SSH 私钥、Kubeconfig、数据库密码或中间件地址写入此目录。
