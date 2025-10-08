# FlexCache Volumes

## 1. FlexCache volume creation

1.1 Data Model Reuse
- We reuse the existing `Volume` table (no new table). A volume is considered a FlexCache when `cache_parameters` (JSONB) is non-nil.
- No new `type` column introduced; detection logic: `volume.CacheParameters != nil`.

1.2 Minimal Scope
- Support creation of FlexCache volumes.
- One origin → many caches (fan-out). Origin in GCNV is not supported.

1.3 API Design
- Re-use existing Volume API with `cache_parameters` field.
- If `cache_parameters` is set, volume is a FlexCache and we will validate accordingly.
- We will use a separate orchestration workflow for FlexCache volumes.
- We will also have a separate temporal workflow for FlexCache volumes with its own activities.

1.4 Workflow Design
- Create a cluster peer if not exists.
- Update volume in DB to surface peering command
- Wait for cluster peer to be available.
- Create an SVM peer if not exists.
- Update volume in DB to surface peering command
- Wait for SVM peer to be available.
- Create the FlexCache volume.
- Wait for the FlexCache volume to be available.
- Update the volume record in DB to mark it as available.

1.5 Error Handling
- Errors during cluster peer or SVM peer creation will not set the volume to failed. The workflow can be retried.
- Errors during FlexCache volume creation will set the volume to failed.
- Errors during peering steps will be surfaced in the `CacheStateDetails` field of the cache parameters
- Once a volume has been marked as failed, it cannot be retried. The user must delete and recreate the volume.
