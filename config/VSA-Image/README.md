# VSA-Image config files

The **VSA and Mediator Compute Image Copy** workflow reads all run parameters from a config file in this folder. When running the workflow, you only provide the config file name plus run mode, image type, and environment.

- **File naming:** Use the release version as the file name: `<version>.json` (e.g. `9.18.1X26.json`).
- **When running:** Enter the **config file name** (required) in the workflow UI. Version, image names (VSA and Mediator), bundle name, and upgrade paths are read from the file. Choose **Image type**: **VSA**, **Mediator**, or **Both** (copies both images and upgrade in one run; if an image is already present, that image is skipped and the rest continue).
- **Format:** See `config/versions/IMAGE_COPY_RUN_FORMAT.md`. Use `image_name` for VSA and `mediator_image_name` for Mediator. Optional: `vlm_worker_path` and `vlm_worker_tag` (VLM worker image push in same run); `rbac_folder_path` (creates RBAC folder in destination and copies `gcnvadmin_create_cli` and `gcnvadmin_create_cli.sha256.b64`). The standalone **VLM Worker Image Push** workflow remains available for VLM-only runs.

Example: `9.18.1X26.json`.
