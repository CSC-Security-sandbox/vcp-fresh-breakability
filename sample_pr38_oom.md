<!-- breakability-check -->
## ❌ BUILD_FAILS — `github.com/lib/pq` 1.11.2 → 1.12.3 · dev · minor

Build: ❌ fails on PR branch, ✅ passes on main · Usage: 0 file(s)

### Build errors (excerpt)
```
  full build: no import data available, building ./...
```

### What to do
1. Check the full build output in the Actions run for this PR
2. Review the `github.com/lib/pq` 1.11.2 → 1.12.3 changelog for breaking changes
📝 [Changelog](https://github.com/lib/pq/compare/v1.11.2...v1.12.3)
3. Fix type errors or update your code to match the new API
4. Re-run the breakability analysis after your fix

**Do not merge — build is broken.** (minor bump)
📋 Merge plan: #79
<details><summary>🔍 How we checked (verification: L2_type_checked)</summary>

- ✅ Dependency resolved — `go get`/`npm install` exit 0
- ❌ Project build fails on PR branch
- ✅ Build passes on main — errors are introduced by this upgrade
- ⬜ Tests not run (build must pass first)
</details>

🔗 [View analysis run](https://github.com/CSC-Security-sandbox/vcp-vsa-breakability-test/actions/runs/26515416643)
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*
