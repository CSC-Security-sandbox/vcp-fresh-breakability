# VSA, Mediator & VLM Image Copy Pipeline — Improvements (Branch: image-cop-fix)

**Purpose:** This document summarizes the improvements made to the VSA and Mediator Compute Image Copy pipeline, including integration of the VLM worker image push and resolution of GCS access restrictions. Use this for the PR description and for stakeholder communication.

---

## 1. Problem Statement

**Before this work:**

- **Fragmented process:** Releasing a new VSA/Mediator version required multiple separate steps: copying VSA image, copying Mediator image, copying upgrade files, and then handling the VLM worker image through a **fully manual** process.
- **Manual VLM workflow:** Pushing the VLM worker Docker image required an engineer to:
  1. SSH into a GCE pull VM.
  2. Set service-account impersonation on the VM.
  3. Manually copy the VLM tar.gz from GCS, load the image, tag it, and push to Artifact Registry.
- **No single pipeline:** There was no single, config-driven run that could perform VSA copy, Mediator copy, upgrade copy, and VLM push together. This increased coordination overhead, risk of human error, and inconsistency between releases.
- **GCS access blocked from CI:** When we first automated VLM copy from GitHub Actions, the run failed with **403 AccessDeniedException** (organization policy / VPC Service Controls) when the runner tried to read from `gs://cot-releases-public/...`. Direct access from the runner is not allowed; access must go through the pull VM with impersonation.

---

## 2. What We Did

- **Single config–driven pipeline**  
  All run parameters (version, VSA image name, Mediator image name, bundle, upgrade paths, and optional VLM path/tag) are read from one JSON config file in `config/VSA-Image/`. Operators only provide the config file name, run mode, image type, and environment.

- **“Both” image type and mediator support**  
  One run can copy both VSA and Mediator images in a single workflow execution. If an image already exists at the destination, that image is skipped and the rest of the pipeline continues. Mediator is explicitly supported via `mediator_image_name` in the config (no incorrect fallback to VSA image name).

- **Mandatory upgrade copy and consistency**  
  The pipeline always copies the upgrade file from the config. When a VSA image is present, the pipeline ensures the upgrade folder/file exists (creates and copies if missing) so VSA and upgrade stay in sync.

- **VLM worker integrated into the same pipeline**  
  When the config file includes optional `vlm_worker_path` and `vlm_worker_tag`, the **same** VSA/Mediator workflow (single-environment run) also runs the “Push VLM Worker Image” job. No separate workflow run is required for a full release.

- **VLM copy via pull VM and impersonation**  
  To comply with org policy (no direct GCS access from the runner), the VLM job:
  1. SSHs into the existing pull VM (same as other image-copy steps).
  2. On the VM: sets impersonation to `gcnv-vsa-image-sa@gcnv-vsa-prod.iam.gserviceaccount.com` and runs `gsutil cp` to download the VLM tar.gz from GCS.
  3. SCPs the file from the VM to the runner.
  4. On the runner: unzips, loads the Docker image, checks if the tag already exists in Artifact Registry, and if not, tags and pushes.

- **Idempotency**  
  If the VLM image tag already exists in the registry, the job skips the push and reports success. VSA/Mediator “Both” runs skip images that are already present.

- **Documentation**  
  Updated `config/VSA-Image/README.md` and `config/versions/IMAGE_COPY_RUN_FORMAT.md` to describe the config format, image types (VSA / Mediator / Both), and the optional VLM keys that trigger the integrated VLM push.

---

## 3. What We Achieved

| Outcome | Description |
|--------|-------------|
| **One run for a full release** | A single pipeline run (single environment) can copy VSA image, Mediator image, upgrade files, and push the VLM worker image when the config includes the VLM keys. |
| **No manual VLM steps** | VLM copy, load, tag, and push are fully automated via the same pipeline; no manual SSH or impersonation required. |
| **Single source of truth** | One config file per release defines all image names, paths, and VLM path/tag, reducing errors and drift. |
| **Policy-compliant GCS access** | VLM artifact is read from GCS only through the pull VM with the designated service account, satisfying org policy / VPC Service Controls. |
| **Consistent upgrade handling** | Upgrade copy is mandatory and aligned with VSA image presence (ensure-upgrade step). |
| **Standalone option retained** | The separate “VLM Worker Image Push” workflow remains available for VLM-only runs when needed. |

---

## 4. Time Saved Compared to Previous Method

| Previous method | New method | Time impact |
|-----------------|-----------|-------------|
| Multiple workflow runs (VSA, then Mediator, or separate runs) | One run with image type “Both” (or single type) | **Fewer runs;** one execution instead of two for VSA + Mediator. |
| Manual SSH to VM, set impersonation, run gsutil, then manual docker load/tag/push for VLM (~15–30+ min per release) | Fully automated VLM step in the same pipeline (copy via VM, then load/tag/push on runner) | **Estimated 15–30+ minutes saved per release** by eliminating manual VLM steps. |
| Coordinating and re-entering version/image/bundle in different places | Single config file; one file name selected in the UI | **Less coordination and data entry;** lower risk of typos and inconsistent versions. |

**Overall:** The pipeline reduces manual effort and coordination per release and removes the need for manual VLM handling, saving an estimated **15–30+ minutes per release** and reducing operational risk.

---

## 5. Key Technical Details

- **Workflow:** `VSA and Mediator Compute Image Copy` (`.github/workflows/vsa-mediator-image-copy.yml`).
- **Config location:** `config/VSA-Image/<version>.json` (e.g. `9.18.1X26.json`).
- **Run mode:** Single environment (for VLM integration) or All environments (autopush → staging → prod).
- **Image type:** VSA, Mediator, or Both.
- **Pull VM:** Same VM used for other image-copy steps (`vsa-pull-image-instance`, project `gcnv-vsa-prod`, zone `us-central1-a`).
- **VLM impersonation SA:** `gcnv-vsa-image-sa@gcnv-vsa-prod.iam.gserviceaccount.com`.
- **VLM registry:** `us-docker.pkg.dev/gcnv-artifact-registry-nonprod/vcp-container-images-us/vlm-worker:<tag>`.

---

## 6. References

- Config format: `config/versions/IMAGE_COPY_RUN_FORMAT.md`
- Config folder: `config/VSA-Image/README.md`
- Standalone VLM workflow: `.github/workflows/vlm-worker-image-push.yml` (for VLM-only runs)
