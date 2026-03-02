# Billing Sanity Checks CLI (tstctl)

A production-grade CLI application for running automated billing sanity checks on VSA Control Plane systems. This tool clones test repositories, executes test suites, parses results, and sends formatted reports to Slack channels.

> **Note**: This project is located at `gcnv-sre-utils/cli-tools/tstctl` within the `gcnv-sre-utils` repository.

## 🚀 Features

- **Automated Test Execution**: Clones test repositories and runs sanity check test suites
- **Multiple Test Types**: Supports CRR, Backup, Pool, and Volume billing sanity checks
- **Slack Integration**: Automatically sends beautifully formatted test results to Slack channels
- **JSON Configuration**: Accepts JSON input for flexible test configuration
- **XML Result Parsing**: Parses and processes test result XML files
- **Environment-Aware**: Supports multiple environments and configurations
- **Docker Support**: Containerized for easy deployment and execution

## 📋 Prerequisites

- Go 1.24.1 or higher
- Docker (for containerized builds)
- Access to the test repository: `github.com/VCP-VSA-control-Plane/vsa-cp-cd.git`
- Slack token and channel ID for notifications
- GitHub Personal Access Token (PAT) for repository cloning

## 🛠️ Installation

### Local Build

```bash
# Clone the repository
git clone <repository-url>
cd gcnv-sre-utils/cli-tools/tstctl

# Install dependencies
go mod download

# Build the binary
go build -o tstctl .
```

## 🐳 Building and Pushing Docker Image

To build and push the Docker image to Google Container Registry (GCR) for Cloud Run deployment, navigate to the tstctl directory and run:

```bash
cd gcnv-sre-utils/cli-tools/tstctl
docker buildx build --platform linux/amd64 -t gcr.io/netapp-us-c1-autopush-sde-tst/sanity/tstctl --push .
```

**Prerequisites for pushing to GCR:**
1. Authenticate with GCP:
   ```bash
   gcloud auth configure-docker
   ```

2. Ensure you have permissions to push to the GCR repository:
   ```bash
   gcloud auth login
   ```

3. Set the correct project:
   ```bash
   gcloud config set project netapp-us-c1-autopush-sde-tst
   ```

**Build Options:**
- `--platform linux/amd64`: Specifies the target platform (required for Cloud Run)
- `-t`: Tags the image with the full GCR path
- `--push`: Pushes the image directly to the registry after building

## 📖 Usage

### Basic Command Structure

```bash
./tstctl sanity [flags]
```

### Available Flags

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--config` | `-c` | Config file path | - |
| `--tests` | `-t` | Test suite path | - |
| `--branch` | `-b` | Git branch to clone | `main` |
| `--slack-channel` | `-s` | Slack channel ID | `C09HUEEJ3NE` |
| `--environment` | `-e` | Target environment | `ap-tst-us-c1` |
| `--pool-id` | `-p` | Pool ID for test resources | - |
| `--jsonInput` | `-j` | JSON string input for configuration | - |

### Example Commands

#### Run with Config File

```bash
./tstctl sanity \
  --config /path/to/config.json \
  --tests tests/ccfe/billing \
  --environment ap-tst-us-c1 \
  --slack-channel C09HUEEJ3NE
```

#### Run with JSON Input

```bash
./tstctl sanity \
  --jsonInput '{"poolName":"test-pool","volumeName":"test-volume"}' \
  --tests tests/ccfe/billing \
  --environment ap-tst-us-c1
```

#### Run with Pool ID

```bash
./tstctl sanity \
  --pool-id pool-12345 \
  --tests tests/ccfe/pool \
  --environment ap-tst-us-c1
```

## 🔧 Environment Variables

The following environment variables must be set for the application to function:

| Variable | Description | Required |
|----------|-------------|----------|
| `GITHUB_PAT` | GitHub Personal Access Token for cloning repositories | ✅ Yes |
| `SLACK_TOKEN` | Slack Bot Token for sending notifications | ✅ Yes |
| `SLACK_CHANNEL_ID` | Slack Channel ID (can be overridden with `--slack-channel` flag) | ✅ Yes |
| `TDS_DOC_URL` | URL to Test Design Specification documentation | ❌ No |
| `POOL_ID` | Pool ID for test resources (can be set via `--pool-id` flag) | ❌ No |
| `POOLNAME` | Pool name (set via JSON input) | ❌ No |
| `VOLUMENAME` | Volume name (set via JSON input) | ❌ No |
| `DESTPOOLNAME` | Destination pool name (set via JSON input) | ❌ No |
| `REPLICATIONID1` | Replication ID 1 (set via JSON input) | ❌ No |
| `REPLICATIONID2` | Replication ID 2 (set via JSON input) | ❌ No |

## 📊 Test Types

The application supports multiple billing sanity check types:

- **Billing Onboarding**: General billing sanity checks
- **CRR Billing**: Cross-Region Replication billing checks
- **Backup Billing**: Backup-related billing validation
- **Pool Billing**: Storage pool billing metrics validation
- **Volume Billing**: Volume billing metrics validation

Test type is automatically detected from the test suite path and appropriate Slack notifications are sent.

## 🔄 Workflow

1. **Preflight Checks**: Validates required environment variables
2. **Repository Cloning**: Clones the test repository from GitHub
3. **Configuration**: Processes JSON input or config file
4. **Test Execution**: Runs the test script with a 3-hour timeout
5. **Result Processing**: Parses XML test results
6. **Slack Notification**: Sends formatted results to the specified Slack channel

## 📁 Project Structure

This project is located at `gcnv-sre-utils/cli-tools/tstctl` within the `gcnv-sre-utils` repository.

```
gcnv-sre-utils/
└── cli-tools/
    └── tstctl/
        ├── cmd/
        │   ├── root.go          # Root command and CLI setup
        │   └── sanity.go        # Sanity check command implementation
        ├── common/
        │   ├── config.go        # Configuration and environment setup
        │   ├── git.go           # Git repository operations
        │   ├── parsing.go       # XML parsing utilities
        │   ├── shell.go         # Shell script execution
        │   └── slack.go         # Slack notification integration
        ├── structs/
        │   └── out.go           # Output structures
        ├── main.go              # Application entry point
        ├── Dockerfile           # Docker build configuration
        ├── go.mod               # Go module dependencies
        └── README.md            # This file
```

## 🧪 Testing

The application executes test scripts from the cloned repository. Test results are parsed from XML output files and formatted for Slack notifications.

## 📝 Slack Notifications

Test results are automatically sent to Slack with:
- ✅ Success/Failure status with color coding
- Test summary and details
- Environment and host information
- Links to relevant documentation
- Timestamp and execution metadata

## 🐛 Troubleshooting

### Common Issues

**Repository Clone Fails**
- Verify `GITHUB_PAT` is set and has repository access
- Check network connectivity to GitHub

**Slack Notifications Not Sent**
- Verify `SLACK_TOKEN` and `SLACK_CHANNEL_ID` are set correctly
- Ensure the Slack bot has permission to post to the channel

**Test Execution Timeout**
- Default timeout is 3 hours (10800 seconds)
- Check test script execution logs for issues

**JSON Input Parsing Errors**
- Ensure JSON is properly formatted
- Check for escaped quotes in the input string

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 🔗 Related Documentation

- [tstctl – Cloud-Native Test Orchestration CLI](https://confluence.ngage.netapp.com/spaces/VSCP/pages/1334142179/tstctl+%E2%80%93+Cloud-Native+Test+Orchestration+CLI) - Main documentation for tstctl CLI
- [Billing Metrics Sanity Checks Setup](https://confluence.ngage.netapp.com/spaces/VSCP/pages/1338924354/Billing+Metrics+Sanity+Checks+Setup)
- [Pool Billing Metrics TDS](https://confluence.ngage.netapp.com/spaces/VSCP/pages/1378911888/Pool+Billing+Metrics+validation+-+Test+Design+Specification+TDS)
- [Volume Billing Metrics TDS](https://confluence.ngage.netapp.com/spaces/VSCP/pages/1378911986/Volume+Billing+Metrics+Validation+-+Test+Design+Specification+TDS)

---

**Built with ❤️ using Go and Cobra CLI**

