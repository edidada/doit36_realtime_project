# 查看本项目 K3s 运行状态

本项目部署在 `doit` 命名空间。

记录时的状态：6 个 Pod 全部 `1/1 Running`，重启次数均为 0；
Flink 作业为 `RUNNING / STABLE`。这是当时的快照，实时状态以下列命令为准。

## 登录 master

在 Mac 上执行：

```bash
ssh root@192.168.31.77
```

后续 `kubectl` 命令均在 master 上执行。

## 查看 Pod

查看 Pod 状态、IP 和所在节点：

```bash
kubectl -n doit get pods -o wide
```

持续观察状态变化，按 `Ctrl+C` 退出：

```bash
kubectl -n doit get pods -w
```

## 查看 Flink 作业与日志

```bash
kubectl -n doit get flinkdeployment realtime-dw-etl-01
kubectl -n doit logs -f deployment/realtime-dw-etl-01
```

预期作业状态为 `RUNNING`，生命周期状态为 `STABLE`。

## 排查异常 Pod

将 `<Pod名称>` 替换为 `get pods` 输出中的实际名称：

```bash
kubectl -n doit describe pod <Pod名称>
kubectl -n doit logs <Pod名称> --tail=100
kubectl -n doit get events --sort-by=.metadata.creationTimestamp
```

## Flink 页面

在可访问集群内网的浏览器中打开：

<http://192.168.31.88:30816/>

可查看作业、TaskManager 和 checkpoint 状态。
