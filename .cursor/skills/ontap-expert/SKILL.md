---
name: ontap-expert
description: >-
  Explains NetApp ONTAP storage features in depth (concepts, architecture,
  operations, limits, REST/CLI). Use when the user runs /ontap, says "ontap
  expert", asks what an ONTAP feature is (FlexCache, SnapMirror, FlexGroup,
  SVM, SnapLock, MetroCluster, quotas, etc.), or wants product knowledge
  separate from API execution (gcnvapis).
---

# ONTAP Feature Expert

## Purpose

Deliver a **complete, structured briefing** on one ONTAP feature per request: what it is, how it works, prerequisites, operations, failure modes, and how it appears in **GCNV / VCP** when applicable.

This is **product and architecture knowledge**, not live API execution. For curl/CCFE/Proxy workflows, use `gcnvapis` (`activate netapp_apis`).

## When to use

- User runs **`/ontap <feature>`** or **`/ontap`** then names a feature
- Phrases: `ontap expert`, `explain ontap`, `what is flexcache`, `how does snapmirror work`
- Comparing two ONTAP features (`flexcache vs flexclone`)

## Research procedure

1. Open **feature-index.md** in this skill directory; find the feature or closest alias.
2. **Repo (VCP)** — read listed `doc/`, workflow, and model files only when the index lists them.
3. **REST** — grep `clients/ontap-rest/swagger.yaml` for the feature; note primary resources (e.g. `/storage/flexcache`).
4. **Official docs** — web search `site:docs.netapp.com ontap 9 <feature>`; fetch 1–2 authoritative pages. Prefer Software Docs over marketing pages.
5. **Synthesis** — merge sources; flag conflicts between repo implementation and generic ONTAP docs.

## Mandatory output template

Use these headings in order (omit a section only if truly N/A, and say why):

### 1. Executive summary
2–4 sentences: problem solved, core idea, typical use case.

### 2. Core concepts
- Key terms (origin, cache, relationship, policy, …)
- What is stored where (cluster, SVM, volume, aggregate)
- Relationship to adjacent features (e.g. FlexCache vs FlexClone vs cache miss)

### 3. Architecture and data flow
- Control plane vs data path (high level)
- Peering / networking / protocol dependencies if any
- ASCII or mermaid diagram when the flow is non-obvious

### 4. Operations (operator view)
| Operation | What happens | Notes |
|-----------|--------------|-------|
| Create / enable | … | … |
| Modify | … | … |
| Delete / disable | … | … |
| Monitor / troubleshoot | … | … |

### 5. Configuration surface
- **ONTAP REST**: resource paths and important fields (from swagger)
- **CLI** (optional): common `::*` commands if user asked or ops care
- **GCNV / VCP** (if applicable): API fields, workflows, env vars from repo docs

### 6. Prerequisites and constraints
- Licensing / edition (if documented)
- Version / platform notes (only if sourced)
- Capacity, count, or performance limits (**cite source** or mark Unknown)

### 7. Failure modes and troubleshooting
- 3–6 common errors or states and what they mean
- What to check first (peer, export policy, name service, space, …)

### 8. In this repository (VCP / GCNV)
Only when feature-index lists paths:
- Workflows / activities / handlers
- Link to `doc/workflows/...` or ADRs
- How a user request becomes ONTAP work (1 short paragraph)

### 9. Related features
Bullet list with one-line distinction (e.g. SnapMirror vs SnapVault vs FlexCache).

### 10. References
- Official doc URLs
- Local paths: `doc/...`, `clients/ontap-rest/swagger.yaml#...`, workflow `.go` files

## Quality rules

- **Evidence labels**: Product fact | VCP implementation | Hypothesis
- No fabricated version numbers, port lists, or license SKUs
- Typo tolerance: normalize feature name once
- Comparisons: use a small table when user asks "X vs Y"
- Keep total length proportional: simple features ~400–800 words; complex (SnapMirror, MetroCluster) up to ~1500

## Examples

**User:** `/ontap flexcache`

**You:** Full template; emphasize origin/cache cluster peering, prepopulate, writeback; VCP section from `doc/workflows/flexcache/` and create/delete workflows.

**User:** `ontap expert snapmirror` with `rest`

**You:** Same template; heavier section 5 with swagger paths under `snapmirror` resources.
