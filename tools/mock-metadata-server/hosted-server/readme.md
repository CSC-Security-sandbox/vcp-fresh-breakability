# Mock Metadata Server

A lightweight mock of the GCP metadata service for local development and testing. This server fetches and serves GCP access tokens using the standard metadata server path.


## Features

- Mimics GCP metadata token endpoint  
- Auto-refreshes tokens before they expire  
- Can be hosted centrally on a shared VM  
- Provides a drop-in replacement for `http://metadata.google.internal`


## Files Required

- `app.go`: The server implementation
- (Generated) `app`: Compiled Go binary
- (Optional) `metadata-server.service`: Systemd unit file


## Hosting Instructions (Any Linux VM)

### 1. Build the binary locally

```bash
go build tools/mock-metadata-server/hosted-server/app.go
scp app user@<target-vm>:/tmp/
```

### 2. SSH into the VM and install

```bash
sudo mkdir -p /opt/metadata-server
sudo mv /tmp/app /opt/metadata-server/
sudo chmod +x /opt/metadata-server/app
```

### 3. Create a systemd service

```bash
sudo nano /etc/systemd/system/metadata-server.service
```

Paste the following content:

```ini

[Unit]
Description=Mock GCP Metadata Server
After=network.target

[Service]
ExecStart=/opt/metadata-server/app -port=9090
WorkingDirectory=/opt/metadata-server
Restart=always
RestartSec=5
User=nobody
Group=nogroup
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### 4. Enable and start the service

```bash
sudo systemctl daemon-reload
sudo systemctl enable metadata-server
sudo systemctl start metadata-server
```

---

##  Developer Environment

Set the following environment variable to point your application to the mock server:

```bash
export GCE_METADATA_HOST=<vm-ip>:9090
```

---

##  Cleanup or Update

To stop or restart the service:

```bash
sudo systemctl stop metadata-server
sudo systemctl restart metadata-server
```