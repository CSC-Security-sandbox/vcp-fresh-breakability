# VCP-Local Triagebot Agents

This directory is the canonical prompt tree for triage inside `vsa-control-plane/`.

## Layout
- `log-fetcher.md` - fetches and normalizes logs for either VCP-only or cross-service mode
- `specialist-vcp.md` - analyzes VCP-local failures
- `specialist-cvs.md` - analyzes CVS failures from the configured external repo
- `specialist-cvp.md` - analyzes CVP failures from the configured external repo
- `specialist-cvn.md` - analyzes CVN failures from the configured external repo
- `cross-boundary-verifier.md` - attributes caller, callee, or boundary fault
- `payload-verifier.md` - runs targeted verification in the correct repo
- `evidence-gate.md` - selects the winning candidate and confidence

## Repo roots
- VCP: `.`
- CVS: `repos[cvs].repo_path`
- CVP: `repos[cvp].repo_path`
- CVN: `repos[cvn].repo_path`

## Notes
- External repo mode is controlled by `.cursor/state/memory.md`.
- `cross_repo=false` skips external repos and downstream specialists.
- `cross_repo=true` enables downstream specialists against the user-provided repo paths.
- If an external repo is missing or invalid, specialists must say so explicitly and lower confidence when needed.
