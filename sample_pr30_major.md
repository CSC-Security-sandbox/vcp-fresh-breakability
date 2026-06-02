<!-- breakability-check -->
## ⚙️ LIKELY SAFE — `k8s.io/client-go` 0.33.3 → 0.35.3 · production · major ⚠️ (0.x unstable — treat as breaking)

Build: ⚙️ same errors on main and PR branch — pre-existing failure, **not caused by this upgrade** · Verification: **L1_dep_resolved**

### What this means
Dependencies resolved successfully. The build fails on both `main` and this PR with the same errors. This upgrade does **not** introduce new failures. Full build verification was limited by pre-existing issues on `main`.

**Recommendation:** Likely safe to merge — no new errors detected. Fix pre-existing build failures on `main` for full verification coverage.
📋 Merge plan: #79
<details><summary>🔍 How we checked (verification: L1_dep_resolved)</summary>

- ✅ Dependency resolved — `go get`/`npm install` exit 0
- ⚠️ Build fails on both `main` (exit=124 (timeout)) and PR branch — same errors
- ⚙️ Pre-existing failure is in `google-proxy-client` — **unrelated** to this PR's package (`k8s.io/client-go`)
- ✅ No NEW errors introduced by this upgrade

**Pre-existing build errors:**
```
github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client: /home/runner/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.0.linux-arm64/pkg/tool/linux_arm64/compile: signal: killed
```

Fix these on `main` to unlock full L2+ verification.
</details>

🔗 [View analysis run](https://github.com/CSC-Security-sandbox/vcp-vsa-breakability-test/actions/runs/26515416643)
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*
