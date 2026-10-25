# R630 K3s 运行清单

这些清单描述 2026-10-25 后的稳定归属：业务作业在 `realtime-dw`，Kafka/HBase/Doris 在 `middleware`。它们不包含密码，也不包含 `local-path` PV 的跨 Namespace 解绑/重绑操作；那一步必须按 [迁移测试报告](../../docs/k3s-migration-test-report-2026-10-25.md) 的备份和回滚步骤单独实施。

## 前置对象

- `middleware` 中已存在并绑定数据的 PVC：`business-hbase-data`、`business-doris-fe-data`、`business-doris-be-data`。
- `middleware/business-doris-auth`：键名为 `password` 的 Secret。
- `realtime-dw/flink-state`：已绑定的 50Gi PVC。
- `realtime-dw/business-etl-job`：键名为 `business-etl.jar` 的 ConfigMap；由当前 Maven 产物创建，不能提交二进制 Jar。
- worker1 有 `/var/lib/doit36/hbase-2.4.18`，worker2 有 Doris no-AVX 二进制目录；这是现有节点绑定设计。

应用顺序：

```bash
kubectl apply -f deploy/k8s/middleware-realtime-stores.yaml
kubectl apply -f deploy/k8s/realtime-dw-flink.yaml
```

不要在没有数据备份的情况下删除 PVC。运行中的现场使用 `Retain` 回收策略，避免 Namespace 删除传播到本地数据目录。
