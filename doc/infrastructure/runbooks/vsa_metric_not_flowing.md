# Runbook for **VSA Harvest Pod Deployment Alert**

This runbook provides a structured approach to debugging alerts during the deployment of VSA harvest pods in a new GCP region.

# Alert Information

| Field            | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| Alert Name       | VSA Harvest Pod Deployment Failure                                          |
| Alert Link       | [Monitoring System Link]                                                    |
| Alert Threshold  | Deployment failure, missing bucket access, or metrics not flowing           |
| Date of Creation | [Date of initial alert configuration]                                       |
| SME              | Piyush Gaur, ShyamPrasad Chitturi                                           |
| Severity         | Critical                                                                    |


# Debugging Steps (Guidelines)

1. **Acknowledge the Alert**

- Acknowledge the alert in the monitoring system to prevent repeated notifications.
- Record the time of acknowledgment.

2. **Gather Initial Context**

- Review the alert description and any associated dashboards or logs.
- Identify the affected service or component.
- Check recent deployments or changes that might have triggered the alert.

**Viewing Logs for Harvest and OpenTelemetry Containers**

To check logs for specific containers in the `vcp` namespace:

- **Harvest Logs**:
  Use the `harvest-pm2` container name.

- **OpenTelemetry Logs**:
  Use the `otel-collector` container name.

# Command Format:

```bash
kubectl -n vcp logs -f <POD_NAME> -c <CONTAINER_NAME>
```

Replace:

- `<POD_NAME>` with the name of the pod you want to inspect.
- `<CONTAINER_NAME>` with either `harvest-pm2` or `otel-collector`.

To confirm that metrics are flowing correctly from each poller:

- Ensure the `port` field under the **Harvest** section is not empty.
- Ensure the `metrics_port` field under the **PM2** section is not empty.

**Steps to Check Poller Configuration:**

1. List active leases:

```bash
kubectl get leases
```

2. Identify the pod name associated with the lease.

3. Exec into the pod:

```bash
kubectl -n vcp exec -it POD_NAME -- bash
```

4. Navigate to the poller configuration directory:

```bash
cd /configs/harvest/harvest-lease-uuid
```

5. View the configuration file:

```bash
cat harvest-node-ud.yaml
```

**Sample YAML Configuration:**

```yaml
Exporters:
  prometheus:
    exporter: Prometheus
    local_http_addr: 0.0.0.0
    port: 13009
  service_control:
    exporter: ServiceControl
    url: https://servicecontrol.googleapis.com
    service_name: autopush-netapp.sandbox.googleapis.com
    mappings:
      volume:
        - source: "size_total"
          target: "netapp.googleapis.com/volume/allocated_bytes"
        - source: "space_logical_used"
          target: "netapp.googleapis.com/volume/bytes_used"
        - source: "snapshots_size_used"
          target: "netapp.googleapis.com/volume/snapshot_bytes"
        - source: "read_ops"
          target: "netapp.googleapis.com/volume/operation_count"
          labels:
            type: "read"
        - source: "write_ops"
          target: "netapp.googleapis.com/volume/operation_count"
          labels:
            type: "write"
        - source: "other_ops"
          target: "netapp.googleapis.com/volume/operation_count"
          labels:
            type: "metadata"
        - source: "read_data"
          target: "netapp.googleapis.com/volume/throughput"
          labels:
            type: "read"
        - source: "write_data"
          target: "netapp.googleapis.com/volume/throughput"
          labels:
            type: "write"
        - source: "other_data"
          target: "netapp.googleapis.com/volume/throughput"
          labels:
            type: "metadata"
        - source: "read_latency"
          target: "netapp.googleapis.com/volume/average_latency"
          labels:
            method: "read"
        - source: "write_latency"
          target: "netapp.googleapis.com/volume/average_latency"
          labels:
            method: "write"
        - source: "other_latency"
          target: "netapp.googleapis.com/volume/average_latency"
          labels:
            method: "metadata"
        - source: "inode_files_used"
          target: "netapp.googleapis.com/volume/inode_used"
        - source: "inode_files_total"
          target: "netapp.googleapis.com/volume/inode_limit"
        - source: "new_status_code"
          target: "netapp.googleapis.com/internal/volume/volume_online"

Defaults:
  collectors:
    - Rest
    - KeyPerf
  use_insecure_tls: false

Pollers:
  cluster7714-gcnv-458fae19df89977-01:
    datacenter: australia-southeast1
    addr: IP_ADDR
    auth_style: basic
    username: admin
    password:
    auth_type: 2
    secret_id: gcnv-458fae19df89977-secret
    secret_project_number: 266893635349
    collectors:
      - Ems
      - Rest
      - RestPerf
      - KeyPerf
    labels:
      - project: 441180080430
    use_insecure_tls: true
    exporters:
      - service_control
      - prometheus

PM2:
  - name: cluster7714-gcnv-458fae19df89977-01
    script: /opt/harvest/bin/poller
    metrics_port: 13009
    conf_path: /opt/harvest/conf
    harvest_conf: /configs/harvest/harvest-ae56b2d7-b889-4a08-8b5b-52e00fb4f164/harvest-9835.yaml
    poller_name: cluster7714-gcnv-458fae19df89977-01
    auto_restart: true
    version: 2.0.0
```

- Identify the affected GKE cluster and namespace.
- Check recent changes in IAM roles, bucket configuration, or YAML files.

3. **Validate the Alert**

- Confirm the alert is not a false positive.
- Verify the metrics and logs that triggered the alert.

4. **Identify the Root Cause**

**Logs Analysis**

- Check for errors like `503 Service Unavailable`, `Poller Offline`, or missing metrics.

**Metrics Review**

- Look for missing or delayed metrics from harvest pods.

To verify the missing or delayed metrics for a specific cluster, first check the poller configuraiton as shared above.

If the configuration is correct, then exec into the pod and try to run the curl command against localhost using the port mentioned in the poller configuration with "/metrics" endpoint.

If you don't find the curl command available, run the commands below to install it.

```
apt update
apt install curl -y
```

```
root@vsa-harvest-otel-69fbbb6dc6-cvwzn:/configs/harvest/harvest-ae56b2d7-b889-4a08-8b5b-52e00fb4f164# curl localhost:13005/metrics | tail -n 10
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  100k    0  100k    0     0  57.8M      0 --:--:-- --:--:-- --:--:-- 98.2M
metadata_collector_numCalls{version="25.09.15",poller="cluster7720-gcnv-6df29d352dfe03a-01",collector="Rest",object="Node",datacenter="australia-southeast1",project="1000931969815",hostname="vsa-harvest-otel-69fbbb6dc6-cvwzn",task="data",interval="180.0000"} 2
metadata_collector_task_time{version="25.09.15",poller="cluster7720-gcnv-6df29d352dfe03a-01",collector="Rest",object="Node",datacenter="australia-southeast1",project="1000931969815",hostname="vsa-harvest-otel-69fbbb6dc6-cvwzn",task="counter",interval="86400.0000"} 798684
metadata_collector_poll_time{version="25.09.15",poller="cluster7720-gcnv-6df29d352dfe03a-01",collector="Rest",object="Node",datacenter="australia-southeast1",project="1000931969815",hostname="vsa-harvest-otel-69fbbb6dc6-cvwzn",task="counter",interval="86400.0000"} 798684
metadata_collector_api_time{version="25.09.15",poller="cluster7720-gcnv-6df29d352dfe03a-01",collector="Rest",object="Node",datacenter="australia-southeast1",project="1000931969815",hostname="vsa-harvest-otel-69fbbb6dc6-cvwzn",task="counter",interval="86400.0000"} 698331
metadata_collector_parse_time{version="25.09.15",poller="cluster7720-gcnv-6df29d352dfe03a-01",collector="Rest",object="Node",datacenter="australia-southeast1",project="1000931969815",hostname="vsa-harvest-otel-69fbbb6dc6-cvwzn",task="counter",interval="86400.0000"} 100326
aggr_object_store_logical_used{datacenter="australia-southeast1",cluster="gcnv-6df29d352dfe03a",project="1000931969815",aggr="aggr1",node="",bin_num="1",tier="Object Store: gcnv-6df29d352dfe03a-gcp-object-store"} 0
aggr_object_store_physical_used{datacenter="australia-southeast1",cluster="gcnv-6df29d352dfe03a",project="1000931969815",aggr="aggr1",node="",bin_num="1",tier="Object Store: gcnv-6df29d352dfe03a-gcp-object-store"} 0
volume_arw_status{datacenter="australia-southeast1",cluster="gcnv-6df29d352dfe03a",project="1000931969815",ArwStatus="Not Monitoring"} 1
metadata_exporter_time{exporter="Prometheus",target="prometheus",datacenter="australia-southeast1",project="1000931969815",hostname="vsa-harvest-otel-69fbbb6dc6-cvwzn",version="25.09.15",poller="cluster7720-gcnv-6df29d352dfe03a-01",task="http"} 314
metadata_exporter_count{exporter="Prometheus",target="prometheus",datacenter="australia-southeast1",project="1000931969815",hostname="vsa-harvest-otel-69fbbb6dc6-cvwzn",version="25.09.15",poller="cluster7720-gcnv-6df29d352dfe03a-01",task="http"} 400
root@vsa-harvest-otel-69fbbb6dc6-cvwzn:/configs/harvest/harvest-ae56b2d7-b889-4a08-8b5b-52e00fb4f164#
```

If you are able to view the metrics here, then follow the other steps to check the logs.

**System Health Check**

- Ensure `vsa-harvest-otel` pods are running.
- Confirm GKE cluster health and resource availability.

**Dependency Check**

- Validate access to GCS buckets and Secret Manager.

**Configuration Review**

- Ensure the following configurations are correct:

**Bucket Verify**

- **Name**: `vcp-harvest-pv-<PROJECT_ID>`
- **Location Type**: Based on Cluster region

**IAM Permissions**

```bash
gcloud storage buckets add-iam-policy-binding gs://BUCKET_NAME \
--member "principal://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/PROJECT_ID.svc.id.goog/subject/ns/vcp/sa/gcnv-harvest-sa" \
--role "roles/storage.objectUser"
```

```bash
gcloud iam service-accounts add-iam-policy-binding GOOGLE_SERVICEACCOUNT \
--role roles/iam.workloadIdentityUser \
--member "serviceAccount:WorkLoadIdentity[vcp/gcnv-harvest-sa]"
```

**GCS FUSE CSI Driver**

```bash
gcloud container clusters update CLUSTER_NAME \
--update-addons GcsFuseCsiDriver=ENABLED \
--location=LOCATION
```

**OpenTelemetry Collector**

Check the OpenTelemetry collector configmap in the cluster to verify there aren't any changes related to the metrics filtering.

5. **Formulate a Hypothesis**

- Based on logs and configuration, hypothesize whether the issue is due to:
  - IAM misconfiguration
  - Bucket access failure
  - Secret Manager access issues
  - YAML misconfiguration

6. **Implement a Solution/Mitigation**

**Temporary Mitigation**

- Restart affected pods.
- Manually rebind IAM roles if missing.

**Permanent Fix**

- Correct IAM bindings.
- Update YAML files with correct port and credentials.
- Reconfigure bucket access and retention policies.

7. **Verify the Fix**

- Monitor metrics and logs to confirm resolution.
- Validate access to GCS and Secret Manager.

8. **Document the Resolution**

- Record root cause and resolution steps.
- Update this runbook with new insights.

# Useful Tools and Resources

- Monitoring System: [Link]
- Logging Platform: [Link]
- Documentation Wiki: [Link]
- Team Communication Channel: [Link]