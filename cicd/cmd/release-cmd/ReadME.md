# Release Command (`release-cmd`)

This folder contains the implementation of commands and logic for managing release processes in the CI/CD pipeline. The commands are designed to handle tasks such as creating release tags, promoting releases, and managing sprint-based development tags.

## Features

1. **Release Management**:
    - Create and manage release branches and tags.
    - Automatically generate release candidates (RC) and development (DEV) tags.

2. **Promotion Logic**:
    - Promote release candidates (RC) to final release tags.
    - Handle hotfix (HF) releases with validation.

3. **Automation**:
    - Automates Git operations like creating and pushing tags.
    - Ensures proper naming conventions for tags and branches.

## Commands

### 1. `release`
Handles the creation of release branches and tags.

- **Usage**:
  ```bash
  ./release-cmd release
  ```
- **Description**:
    - Fetches the latest tags.
    - Creates a new release branch if it doesn't exist.
    - Generates RC and DEV tags for the sprint.

### 2. `promotional`
Handles the promotion of release candidates (RC) or hotfix (HF) tags to final release tags.

- **Usage**:
  ```bash
  ./release-cmd promotional
  ```
- **Description**:
    - Validates the base release tag.
    - Promotes the tag to a final release or hotfix tag based on the operation type.

## Configuration

The `promotional.yaml` file is used to configure the promotion process.

- **Fields**:
    - `base_tag`: The base tag to promote (e.g., RC or HF tag).
    - `target_tag`: The final tag to create.
    - `operation_type`: The type of operation (`HFFirstRelease` or `FinalTag`).

## Environment Variables

- `GITHUB_REPOSITORY`: The GitHub repository URL.
- `GH_PAT`: GitHub Personal Access Token for authentication.
- `GITHUB_OUTPUT`: File path to store output variables.

## Key Functions

- **`ReleaseFunc`**:
    - Manages the creation of release branches and tags.
- **`PromotionalFunc`**:
    - Validates and promotes tags based on the operation type.
- **`FetchDevTag`**:
    - Retrieves the latest development tag.
- **`CheckForHFfinalName`**:
    - Validates hotfix naming conventions.

## Prerequisites

- Install the `gh` CLI tool for GitHub operations.
- Ensure Git is installed and configured.
- Set the required environment variables.

## Example Workflow

1. **Create a Release**:
   ```bash
   ./release-cmd release
   ```

2. **Promote a Tag**:
   Update `promotional.yaml` with the required values and run:
   ```bash
   ./release-cmd promotional
   ```

## Error Handling

- The commands log errors and terminate the process if critical issues occur (e.g., invalid tags, Git errors).
- Ensure proper error messages are reviewed in the logs for troubleshooting.

This folder simplifies the release process by automating repetitive tasks and enforcing best practices for tag and branch management.