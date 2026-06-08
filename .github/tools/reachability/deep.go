// Command reachability performs Go function-level call-graph reachability analysis
// for the breakability pipeline (Layer 5). It answers: "from this repo's own
// entrypoints (main/init/exported funcs/methods/tests), is a changed dependency
// symbol transitively reachable in the static call graph?"
//
// CALLGRAPH POLICY (see lite.py for full context)
// ================================================
// This tool is the SECOND-TIER reachability check. Use it when:
//   1. The first tier (lite.py) yields UNCERTAIN on a high-risk PR, or
//   2. Whole-repo static analysis is required for policy validation or deep review
//   3. The cost (30-300s per repo) is justified by the decision weight
//
// Do NOT use for routine PR analysis; lite.py's lightweight deterministic approach
// is sufficient for most cases and scales to all PRs. deep.go is targeted, not
// typical dev workflow.
//
// It deliberately mirrors govulncheck's proven pipeline: load SSA, seed with CHA,
// refine twice with VTA, then forward-search from entrypoints to the target sinks.
//
// CRITICAL HONESTY RULE: static analysis is blind to reflection, unsafe, cgo and
// code generation. A "not reachable" result is therefore NEVER asserted as SAFE.
// When the repo contains those dynamic constructs we downgrade an unreachable
// result to POTENTIALLY_REACHABLE so a merge gate never trusts a false negative.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

type targetResult struct {
	Reachable        bool     `json:"reachable"`
	NamedReachable   bool     `json:"named_reachable"`
	ReachableSymbols []string `json:"reachable_symbols"`
	SamplePath       string   `json:"sample_path,omitempty"`
}

type output struct {
	OK             bool                    `json:"ok"`
	Verdict        string                  `json:"verdict"`
	Error          string                  `json:"error,omitempty"`
	DynamicPresent bool                    `json:"dynamic_present"`
	DynamicReasons []string                `json:"dynamic_reasons"`
	Entrypoints    int                     `json:"entrypoints"`
	ModulePath     string                  `json:"module_path,omitempty"`
	Targets        map[string]targetResult `json:"targets"`
}

func main() {
	var (
		repo     = flag.String("repo", ".", "path to the repo module to analyze")
		targets  = flag.String("targets", "", "comma-separated dependency import paths (sinks)")
		symbols  = flag.String("symbols", "", "comma-separated changelog-named exported symbol leaf-names (optional)")
		outPath  = flag.String("out", "", "output JSON path (default stdout)")
		timeout  = flag.Int("timeout", 240, "overall timeout in seconds")
		patterns = flag.String("patterns", "./...", "package load patterns within the repo")
	)
	flag.Parse()

	out := output{Targets: map[string]targetResult{}, DynamicReasons: []string{}}
	targetList := splitNonEmpty(*targets)
	symbolSet := map[string]bool{}
	for _, s := range splitNonEmpty(*symbols) {
		symbolSet[s] = true
	}
	if len(targetList) == 0 {
		emit(out.fail("no targets provided"), *outPath)
		return
	}

	done := make(chan output, 1)
	go func() { done <- analyze(*repo, *patterns, targetList, symbolSet) }()
	select {
	case res := <-done:
		emit(res, *outPath)
	case <-time.After(time.Duration(*timeout) * time.Second):
		emit(out.fail("timeout"), *outPath)
	}
}

func analyze(repo, patterns string, targetList []string, symbolSet map[string]bool) output {
	out := output{Targets: map[string]targetResult{}, DynamicReasons: []string{}}

	// Dynamic-hazard detection runs even if the call graph fails, so the caller
	// always learns whether a "not reachable" result would be trustworthy.
	// This enforces the CRITICAL HONESTY RULE: unreachable + dynamic = downgrade
	// to POTENTIALLY_REACHABLE to avoid false-negative merge gate decisions.
	out.DynamicPresent, out.DynamicReasons = detectDynamic(repo)

	cfg := &packages.Config{
		Mode:  packages.LoadAllSyntax,
		Dir:   repo,
		Tests: true,
	}
	pkgs, err := packages.Load(cfg, patterns)
	if err != nil {
		return out.failKeepDynamic(fmt.Sprintf("packages.Load: %v", err))
	}
	if packages.PrintErrors(pkgs) > 0 {
		// Build/type errors mean we cannot trust the SSA; bail to a fallback verdict.
		return out.failKeepDynamic("package load reported errors (repo does not type-check)")
	}
	if len(pkgs) > 0 {
		out.ModulePath = modulePath(pkgs)
	}

	prog, _ := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()

	entries := entryPoints(prog, pkgs, out.ModulePath)
	out.Entrypoints = len(entries)
	if len(entries) == 0 {
		return out.failKeepDynamic("no entrypoints found")
	}

	cg, err := buildCallGraph(prog, entries)
	if err != nil {
		return out.failKeepDynamic(fmt.Sprintf("call graph: %v", err))
	}

	// Forward-reachable set from entrypoints over the refined call graph.
	reachable := forwardReachable(cg, entries)

	anyReachable := false
	for _, tp := range targetList {
		tr := targetResult{ReachableSymbols: []string{}}
		symSet := map[string]bool{}
		var sampleSink *ssa.Function
		for fn, node := range cg.Nodes {
			if fn == nil || node == nil || !reachable[node] {
				continue
			}
			if fnPackagePath(fn) != tp {
				continue
			}
			name := fn.Name()
			if !isExportedName(name) {
				continue
			}
			if len(symbolSet) > 0 && symbolSet[name] {
				tr.NamedReachable = true
			}
			if !symSet[name] {
				symSet[name] = true
				if sampleSink == nil {
					sampleSink = fn
				}
			}
		}
		if len(symSet) > 0 {
			tr.Reachable = true
			anyReachable = true
			for s := range symSet {
				tr.ReachableSymbols = append(tr.ReachableSymbols, s)
			}
			sort.Strings(tr.ReachableSymbols)
			tr.SamplePath = samplePath(cg, entries, sampleSink)
		}
		out.Targets[tp] = tr
	}

	out.OK = true
	out.Verdict = verdict(anyReachable, out.DynamicPresent)
	return out
}

// verdict encodes the never-assert-SAFE rule: an unreachable result is downgraded
// to POTENTIALLY_REACHABLE whenever dynamic constructs could hide a real path.
func verdict(anyReachable, dynamic bool) string {
	if anyReachable {
		return "REACHABLE"
	}
	if dynamic {
		return "POTENTIALLY_REACHABLE"
	}
	return "UNREACHABLE"
}

func buildCallGraph(prog *ssa.Program, entries []*ssa.Function) (*callgraph.Graph, error) {
	entrySet := make(map[*ssa.Function]bool, len(entries))
	for _, e := range entries {
		entrySet[e] = true
	}
	// CHA seed (sound over-approximation) -> forward-slice to entries -> VTA x2.
	initial := cha.CallGraph(prog)
	fslice := sliceFunctions(forwardReachable(initial, entries))
	vtaCg := vta.CallGraph(fslice, initial)
	fslice = sliceFunctions(forwardReachable(vtaCg, entries))
	cg := vta.CallGraph(fslice, vtaCg)
	cg.DeleteSyntheticNodes()
	return cg, nil
}

func forwardReachable(cg *callgraph.Graph, entries []*ssa.Function) map[*callgraph.Node]bool {
	seen := map[*callgraph.Node]bool{}
	var queue []*callgraph.Node
	for _, e := range entries {
		if n := cg.Nodes[e]; n != nil && !seen[n] {
			seen[n] = true
			queue = append(queue, n)
		}
	}
	if cg.Root != nil && !seen[cg.Root] {
		seen[cg.Root] = true
		queue = append(queue, cg.Root)
	}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, e := range n.Out {
			if e.Callee != nil && !seen[e.Callee] {
				seen[e.Callee] = true
				queue = append(queue, e.Callee)
			}
		}
	}
	return seen
}

func sliceFunctions(nodes map[*callgraph.Node]bool) map[*ssa.Function]bool {
	out := make(map[*ssa.Function]bool, len(nodes))
	for n := range nodes {
		if n != nil && n.Func != nil {
			out[n.Func] = true
		}
	}
	return out
}

func samplePath(cg *callgraph.Graph, entries []*ssa.Function, sink *ssa.Function) string {
	if sink == nil {
		return ""
	}
	target := cg.Nodes[sink]
	if target == nil {
		return ""
	}
	for _, e := range entries {
		start := cg.Nodes[e]
		if start == nil {
			continue
		}
		path := callgraph.PathSearch(start, func(n *callgraph.Node) bool { return n == target })
		if len(path) > 0 {
			var names []string
			names = append(names, shortName(start.Func))
			for _, edge := range path {
				if edge.Callee != nil {
					names = append(names, shortName(edge.Callee.Func))
				}
			}
			return strings.Join(dedupAdjacent(names), " → ")
		}
	}
	return ""
}

// entryPoints follows govulncheck's selection: main+init in main packages, and all
// exported functions/methods in module-local library packages, plus test functions.
func entryPoints(prog *ssa.Program, pkgs []*packages.Package, modPath string) []*ssa.Function {
	var entries []*ssa.Function
	seen := map[*ssa.Function]bool{}
	add := func(f *ssa.Function) {
		if f != nil && !seen[f] {
			seen[f] = true
			entries = append(entries, f)
		}
	}
	for _, sp := range prog.AllPackages() {
		if sp == nil || sp.Pkg == nil {
			continue
		}
		path := sp.Pkg.Path()
		if modPath != "" && !isLocalPath(path, modPath) {
			continue
		}
		isMain := sp.Pkg.Name() == "main"
		for name, mem := range sp.Members {
			fn, ok := mem.(*ssa.Function)
			if !ok {
				continue
			}
			if isMain && (name == "main" || name == "init") {
				add(fn)
				continue
			}
			if isMain {
				continue
			}
			if fn.Object() != nil && fn.Object().Exported() {
				add(fn)
			}
			if strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Benchmark") || strings.HasPrefix(name, "Fuzz") {
				add(fn)
			}
		}
		// Exported methods of exported named types.
		for _, mem := range sp.Members {
			t, ok := mem.(*ssa.Type)
			if !ok || t.Object() == nil || !t.Object().Exported() {
				continue
			}
			ms := prog.MethodSets.MethodSet(t.Type())
			for i := 0; i < ms.Len(); i++ {
				sel := ms.At(i)
				if !sel.Obj().Exported() {
					continue
				}
				if m := prog.MethodValue(sel); m != nil {
					add(m)
				}
			}
		}
	}
	return entries
}

// detectDynamic scans source for constructs that make static reachability unsound.
// These include reflection, unsafe operations, cgo, plugin loading, code generation,
// and go:linkname directives. Their presence means an "unreachable" verdict cannot
// be trusted (downgraded to POTENTIALLY_REACHABLE by verdict() to protect the
// merge gate from false negatives).
func detectDynamic(repo string) (bool, []string) {
	reasons := map[string]bool{}
	markers := map[string][]string{
		"reflect":     {"\"reflect\""},
		"unsafe":      {"\"unsafe\""},
		"cgo":         {"\"C\"", "import \"C\""},
		"plugin":      {"\"plugin\""},
		"go:generate": {"//go:generate"},
		"go:linkname": {"//go:linkname"},
	}
	_ = walkGoFiles(repo, func(_ string, content string) {
		for reason, needles := range markers {
			for _, n := range needles {
				if strings.Contains(content, n) {
					reasons[reason] = true
				}
			}
		}
	})
	var list []string
	for r := range reasons {
		list = append(list, r)
	}
	sort.Strings(list)
	return len(list) > 0, list
}

func walkGoFiles(root string, fn func(path, content string)) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		full := root + string(os.PathSeparator) + name
		if e.IsDir() {
			if name == "vendor" || name == ".git" || name == "node_modules" || name == "testdata" {
				continue
			}
			_ = walkGoFiles(full, fn)
			continue
		}
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		b, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		fn(full, string(b))
	}
	return nil
}

// ── helpers ────────────────────────────────────────────────────────────────
func fnPackagePath(fn *ssa.Function) string {
	if fn == nil || fn.Pkg == nil || fn.Pkg.Pkg == nil {
		return ""
	}
	return fn.Pkg.Pkg.Path()
}

func shortName(fn *ssa.Function) string {
	if fn == nil {
		return "?"
	}
	pp := fnPackagePath(fn)
	if pp == "" {
		return fn.Name()
	}
	seg := pp[strings.LastIndex(pp, "/")+1:]
	return seg + "." + fn.Name()
}

func isExportedName(name string) bool {
	if name == "" {
		return false
	}
	r := name[0]
	return r >= 'A' && r <= 'Z'
}

func isLocalPath(path, modPath string) bool {
	return path == modPath || strings.HasPrefix(path, modPath+"/")
}

func modulePath(pkgs []*packages.Package) string {
	for _, p := range pkgs {
		if p.Module != nil && p.Module.Path != "" {
			return p.Module.Path
		}
	}
	return ""
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func dedupAdjacent(names []string) []string {
	var out []string
	for i, n := range names {
		if i == 0 || names[i-1] != n {
			out = append(out, n)
		}
	}
	return out
}

func emit(o output, path string) {
	b, _ := json.MarshalIndent(o, "", "  ")
	if path == "" {
		fmt.Println(string(b))
		return
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		fmt.Println(string(b))
	}
}

func (o output) fail(msg string) output {
	o.OK = false
	o.Verdict = "UNKNOWN"
	o.Error = msg
	if o.Targets == nil {
		o.Targets = map[string]targetResult{}
	}
	if o.DynamicReasons == nil {
		o.DynamicReasons = []string{}
	}
	return o
}

func (o output) failKeepDynamic(msg string) output {
	dp, dr := o.DynamicPresent, o.DynamicReasons
	o = o.fail(msg)
	o.DynamicPresent, o.DynamicReasons = dp, dr
	return o
}
