# reach — scoped, sound Go reachability prover

Answers one bounded question per dependency bump: **are the API-diff-flagged
symbols actually reachable from this module's code?** — using a real call graph
(`go/ssa` + RTA), not name-grep.

Two distinct signals per symbol:

| Signal | Meaning | Break type it gates |
|---|---|---|
| `direct_in_module` (+ `direct_sites` file:line) | our code calls the symbol directly | compile / signature break |
| `transitively_reachable` | the dependency reaches the symbol internally on our behalf | behavioral-change exposure |

RTA resolves interface and func-value dispatch, so it does not suffer the
indirect-call false-negative that name-grep does.

## Why it is cheap (not the "monorepo call graph takes forever" problem)

The query is **per-bump, one module, a fixed symbol set** — not a continuous
whole-repo graph for every symbol. Measured: ~25-28s wall for the entire
`vsa-control-plane` root module (146k SSA roots); a small module is 1-3s.

## Usage

```
reach -module . -dep github.com/jackc/pgx/v5 \
  -symbols "SecretKey,ScanInt64,PlanScan,CancelRequest" -tests=false
```

Exit 0 = analysis completed (read the JSON). Exit 1 = analysis failed; the caller
MUST treat failure as **unknown** and fall back to the conservative path — never
as proof of safety.
