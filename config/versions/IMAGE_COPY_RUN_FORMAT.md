# Image copy run config file format

The **VSA and Mediator Compute Image Copy** workflow **always** reads all run parameters from a config file. Config files live in **`config/VSA-Image/`** and should be named **`<version>.json`** (e.g. `9.18.1X26.json`). When running the workflow, you provide only: **Config file name**, **Run mode**, **Image type** (VSA, Mediator, or **Both**), and **Environment**. Version, image names, bundle name, and upgrade paths are read from the file. **Both** copies VSA and Mediator in one run; if an image already exists it is skipped and the pipeline continues with the next.

**Upgrade folder is mandatory** in the config file; the pipeline always copies the upgrade file. When the config includes **`vlm_worker_path`** and **`vlm_worker_tag`**, the pipeline (single-environment run only) also pushes the VLM worker image to Artifact Registry. When **`rbac_folder_path`** is set, the pipeline creates the RBAC folder in the destination bucket (if missing) and copies **gcnvadmin_create_cli** and **gcnvadmin_create_cli.sha256.b64** from COT (or source bucket) to `gs://<destination_bucket>/GCNV/<version>/RBAC/`.

---

## Two supported formats

### 1. Release-bundle format (matches release email)

Copy the information from the **release notification email** (bucket details, Name/ImageId table, and object key list) into a JSON file. The pipeline derives version, image name, bundle name, and upgrade path from it. Optional `vlm_worker_path` and `vlm_worker_tag` (flat or release-bundle config) enable VLM worker push in the same run (single-environment only).

**Required keys:**

| Key | Source in email | Example |
|-----|-----------------|---------|
| `release_bucket_prefix` | "Bucket Details: GCNV_released_bundle/R9.18.1-RC" | `"GCNV_released_bundle/R9.18.1-RC"` |
| `release_version` | Version in paths (e.g. 9.18.1) | `"9.18.1"` |
| `images` | Name \| ImageId table | `[{"name": "x-9-18-1", "image_id": "6676482894704159168"}, {"name": "cvo-mediator-x-9-18-1", "image_id": "2463501224935155808"}]` |
| `upgrade_file_path` | GCNV path to `cot.image.ONTAP-*.tgz` in the object list | `"GCNV/9.18.1/cot.image.ONTAP-9.18.1.tgz"` |
The pipeline derives: `bundle_name` from the last segment of `release_bucket_prefix`, `image_name` from `images[]` based on **Image type** in the UI (vsa → name starting with `x-`, mediator → name starting with `cvo-mediator`), and uses `release_bucket_prefix` as `upgrade_release_bundle_path`.

**Example:** use these keys in a JSON file in `config/VSA-Image/` (e.g. `9.18.1.json`) if you copy from the release email.

### 2. Flat format

Use when you are not copying from the email. All of the following are **required** in the config file.

| Key | Type | Description | Example |
|-----|------|-------------|---------|
| `version` | string | Image version | `"9.18.1X26"` |
| `image_name` | string | GCE image name for **VSA** (used when Image type = VSA) | `"x-9-18-1x26"` |
| `mediator_image_name` | string | GCE image name for **Mediator** (used when Image type = Mediator). Optional; if missing, `image_name` is used. | `"cvo-mediator-x-9-18-1x26"` |
| `bundle_name` | string | Bundle name under GCNV_released_bundle | `"9.18.1X26-internal"` |
| `upgrade_release_bundle_path` | string | Top-level release bundle path in COT (same as bucket prefix) | `"GCNV_released_bundle/R9.18.1-RC"` |
| `upgrade_file_path` | string | Path under that bundle to the upgrade tarball | `"GCNV/9.18.1/cot.image.ONTAP-9.18.1.tgz"` |
| `vlm_worker_path` | string | **Optional.** Relative path to VLM worker tar.gz (under same bundle as upgrade). When set with `vlm_worker_tag`, the VSA/Mediator pipeline (single-environment) also pushes the VLM worker image. Full path = `gs://cot-releases-public/<upgrade_release_bundle_path>/<vlm_worker_path>`. | `"VLM/R9.18.1Px/vlm-worker/R9.18.1Px_8002425_260228_1011/vlm-worker_R9.18.1Px_8002425.tar.gz"` |
| `vlm_worker_tag` | string | **Optional.** Tag for the VLM worker Docker image when pushing to Artifact Registry. Required if `vlm_worker_path` is set. | `"R9.18.1Px_8002425"` |
| `rbac_folder_path` | string | **Optional.** Path under `upgrade_release_bundle_path` where RBAC files live. When set, the pipeline creates `GCNV/<version>/RBAC/` in the destination bucket (if missing) and copies `gcnvadmin_create_cli` and `gcnvadmin_create_cli.sha256.b64`. Full COT path = `gs://cot-releases-public/<upgrade_release_bundle_path>/<rbac_folder_path>/`. | `"VLM/R9.18.1Px/RBAC/R9.18.1Px_8002425_260228_1011"` |

**Example:** see `config/VSA-Image/9.18.1X26.json` and `9.18.1P1X5.json`. With `vlm_worker_path` and `vlm_worker_tag` set, a single-environment run also pushes the VLM worker image. With `rbac_folder_path` set, the run also copies RBAC files to the destination bucket. For VLM-only runs, use the **VLM Worker Image Push** workflow with the same config file name.

---

## What to copy from the release email

From the **Bucket Details** line (e.g. "Bucket Details: GCNV_released_bundle/R9.18.1-RC (cot-releases-public)"):

- **release_bucket_prefix:** `GCNV_released_bundle/R9.18.1-RC`
- **release_version:** the version in the paths, e.g. `9.18.1`

From the **Name | ImageId** table:

- **images:** list of `{"name": "<Name>", "image_id": "<ImageId>"}` (e.g. `x-9-18-1`, `cvo-mediator-x-9-18-1`).

From the **object key list** (or SubPackages):

- **upgrade_file_path:** the key for the GCNV ONTAP upgrade tarball, e.g. `GCNV/9.18.1/cot.image.ONTAP-9.18.1.tgz`.

Save as a JSON file in **`config/VSA-Image/`** with a name like `<version>.json`, and enter that **config file name** when running the workflow. The pipeline will read the file and run with the upgrade folder copy enabled.

---

## Usage in the workflow

1. Put your config JSON in **`config/VSA-Image/`** and name it **`<version>.json`** (e.g. `9.18.1X26.json`). See `config/VSA-Image/README.md`.
2. When running the workflow, enter **Config file name** (required, e.g. `9.18.1X26.json`), then choose **Run mode**, **Image type** (VSA or Mediator), and **Environment** (for single-environment runs). No other parameters are asked; everything is read from the config file.
