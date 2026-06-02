<!-- breakability-check -->
## ✅ SAFE — `github.com/sirupsen/logrus` 1.9.3 → 1.9.4 · production · patch

Build: ✅ passes · Verification: **L4_tests_pass** · Usage: 1 file(s) · Module: `automations/tstctl`

patch bump with passing build. No new type errors introduced.
📋 Merge plan: #79
<details><summary>🔍 How we checked (verification: L4_tests_pass)</summary>

- ✅ Dependency resolved — `go get`/`npm install` exit 0
- ✅ Build passes — `targeted build (automations/tstctl module): 1 dirs` — exit 0, 0 new error(s)
- ✅ Tests pass (exit=0) — no regressions vs main
- ✅ Diffed error output: PR introduces 0 new diagnostics
- ℹ️ go.sum: 2 new transitive deps: github.com/sirupsen/logrus,github.com/stretchr/testify
- ✅ govulncheck: PR introduces **no new vulnerabilities** (8 pre-existing on main — unaffected by this PR)
</details>
<details><summary>📂 Files importing this package (1 file(s))</summary>

- `automations/tstctl/cmd/root.go`
</details>
<details><summary>🖥️ Build output (last lines)</summary>

```
  targeted build (automations/tstctl module): 1 dirs
    dirs: ./cmd/...
```
</details>
> 🔬 **Advisory mode** — This analysis is informational. No merges are blocked.

🔗 [View analysis run](https://github.com/CSC-Security-sandbox/vcp-vsa-breakability-test/actions/runs/26515416643)
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*
