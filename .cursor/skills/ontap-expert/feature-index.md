# ONTAP feature index (VCP repo + doc pointers)

Aliases are lowercase; match user input with fuzzy spelling (flexcaxhe → FlexCache).

| Feature | Aliases | Official docs (start here) | ONTAP REST (swagger grep) | VCP / GCNV in this repo |
|---------|---------|---------------------------|---------------------------|-------------------------|
| **FlexCache** | flex cache, cache volume | [FlexCache overview](https://docs.netapp.com/us-en/ontap/flexcache/index.html) | `flexcache`, `container_volume_flexcache` | `doc/workflows/flexcache/flexcache-workflows.md`, `core/orchestrator/workflows/flexcache_workflows/`, `core/orchestrator/activities/flexcache_activities/`, `core/vsa/flexcache.go`, `google-proxy` volume cache fields |
| **SnapMirror** | mirror, smv, dp volume | [SnapMirror](https://docs.netapp.com/us-en/ontap/data-protection/snapmirror-concept.html) | `snapmirror`, `relationship` | `doc/workflows/replication/replication-workflows.md`, `core/orchestrator/workflows/replicationWorkflows/` |
| **SnapVault** | vault, backup to vault | [SnapVault](https://docs.netapp.com/us-en/ontap/data-protection/snapvault-concept.html) | `snapmirror` (vault type) | Backup / replication docs under `doc/workflows/` |
| **FlexClone** | clone, flex clone | [FlexClone](https://docs.netapp.com/us-en/ontap/volumes/concept_flexclone_volume.html) | `volume` clone fields | Volume create workflows, snapshot restore paths |
| **FlexGroup** | flex group | [FlexGroup](https://docs.netapp.com/us-en/ontap/flexgroups/concept_flexgroup_volumes.html) | `style: flexgroup` | Expert mode / volume create (`style` in ONTAP API examples in `gcnvapis.mdc`) |
| **FlexVol** | flexvol, volume | [FlexVol volumes](https://docs.netapp.com/us-en/ontap/volumes/concept_flexvol_volumes.html) | `storage/volumes` | `core/orchestrator/workflows/volume*`, `doc/workflows/core/volume-workflows.md` |
| **SVM** | vserver, storage vm | [SVM](https://docs.netapp.com/us-en/ontap/concepts/storage-virtual-machine-concept.html) | `svm` | Pool/SVM provisioning in pool workflows |
| **Aggregate** | aggr | [Aggregates](https://docs.netapp.com/us-en/ontap/concepts/aggregates-concept.html) | `aggregates` | Pool / cluster capacity in hyperscaler + pool docs |
| **Snapshot** | snap | [Snapshots](https://docs.netapp.com/us-en/ontap/snapshots/index.html) | `snapshots` | `doc/workflows/core/snapshot-workflows.md`, snapshot workflows |
| **SnapLock** | worm, compliance lock | [SnapLock](https://docs.netapp.com/us-en/ontap/snaplock/index.html) | `snaplock` | Search repo for snaplock if user asks GCNV support |
| **MetroCluster** | mcc, stretch cluster | [MetroCluster](https://docs.netapp.com/us-en/ontap/metrocluster/) | `metrocluster` | Usually outside GCNV scope — say so unless repo hits |
| **Qtrees** | qtree | [Qtrees](https://docs.netapp.com/us-en/ontap/qtrees/index.html) | `qtrees` | Quota / volume subtree features |
| **Quotas** | quota, quota policy | [Quotas](https://docs.netapp.com/us-en/ontap/quota-rules/index.html) | `quota` | `google-proxy` quota rules, quota workflows |
| **NFS export** | export policy | [NFS](https://docs.netapp.com/us-en/ontap/nfs-config/index.html) | `export-policy`, `nfs` | Volume export in volume activities |
| **CIFS / SMB** | smb, share | [CIFS](https://docs.netapp.com/us-en/ontap/cifs-config/index.html) | `cifs` | Active Directory workflows `doc/workflows/core/adc-workflows.md` |
| **CIFS AD / LDAP** | active directory | [CIFS AD](https://docs.netapp.com/us-en/ontap/cifs-config/configure-active-directory-domain-name-system.html) | `active-directory` | `core/orchestrator/workflows/adc_workflow.go`, `core/orchestrator/workflows/active_directory_workflows.go`, `doc/workflows/core/adc-workflows.md` |
| **Encryption / CMEK** | kmip, encryption | [Encryption](https://docs.netapp.com/us-en/ontap/encryption/index.html) | `encryption`, `key-manager` | `doc/workflows/kms/kms-workflows.md`, `core/orchestrator/activities/kms_activities/` |
| **SnapRestore** | restore | [SnapRestore](https://docs.netapp.com/us-en/ontap/snapshots/snaprestore-concept.html) | `snaprestore` | Snapshot restore workflows |
| **FabricPool** | cloud tier | [FabricPool](https://docs.netapp.com/us-en/ontap/fabricpool/index.html) | `fabricpool` | Tiering — confirm GCNV support before claiming |
| **Storage efficiency** | dedupe, compression, compaction | [Storage efficiency](https://docs.netapp.com/us-en/ontap/storage-efficiency/index.html) | `efficiency` | Pool / volume settings in API specs |
| **Application groups / AAG** | ag, host group | Host group docs | varies | `google-proxy` host group APIs |
| **Backup (ONTAP-native)** | backup policy | [Backup](https://docs.netapp.com/us-en/ontap/backups/index.html) | `backup` | `doc/workflows/core/backup-workflows.md`, `doc/workflows/background/scheduled-backup-workflows.md` |
| **Replication (GCNV)** | cross region replication | GCNV docs | GCNV + ONTAP | `doc/workflows/replication/replication-workflows.md`, `core/orchestrator/workflows/replicationWorkflows/`, `.cursor/triagebot-agents/vcp/specialist-replication.md` |
| **Expert mode** | ontap api passthrough | GCNV ONTAP API | full `clients/ontap-rest/swagger.yaml` | `gcnvapis.mdc`, `ontap-proxy/`, `doc/architecture/designs/0018-ontap-proxy-api-translation.md` |

## Adding a new row

When implementing a new ONTAP-facing feature in VCP, add a row here with:
1. Official doc URL
2. Swagger resource prefix
3. `doc/workflows/...` and workflow package path
