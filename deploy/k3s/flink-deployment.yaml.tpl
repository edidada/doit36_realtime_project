apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: realtime-dw-etl-01
  namespace: doit
spec:
  image: doit36/flink-jobs:<IMAGE_TAG>
  imagePullPolicy: IfNotPresent
  flinkVersion: v1_19
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
    jarURI: local:///opt/flink/usrlib/realtime_dw/realtime_dw-job.jar
    entryClass: com.test.bigdata.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim
    parallelism: 1
    upgradeMode: stateless
    state: running
