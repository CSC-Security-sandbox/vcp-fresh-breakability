# Triagebot Memory

This file is human-readable operational state for the VCP-local triagebot.

Only the fenced YAML blocks below should be machine-written.

## Triage config
```yaml
triage_config:
  cross_repo: true
```

## Repo registry
```yaml
repos:
  - service: cvs
    repo_path: "/Users/sahoor/Downloads/Repo/cloud-volumes-service"
  - service: cvp
    repo_path: "/Users/sahoor/Downloads/Repo/cloud-volumes-proxy"
  - service: cvn
    repo_path: "/Users/sahoor/Downloads/Repo/cloud-volumes-network"
```

## Run notes
- Last stale-run warning:
- Last manual override:
