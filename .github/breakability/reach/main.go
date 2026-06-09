// Command reach is a scoped, sound reachability prover for Go dependency upgrades.
//
// Given a single bumped module and a set of exported symbols that an API-diff
// flagged as changed/removed in the upgraded dependency, it answers ONE bounded
// question: "is any of those symbols actually reachable from this module's code?"
//
// Unlike name-grep (which misses indirect/interface dispatch and so can produce a
// false 'not reached'), this builds SSA for the module and runs RTA
// (Rapid Type Analysis), which resolves interface and func-value dispatch. The
// query is scoped to ONE module per bump, so it stays cheap even in a monorepo —
// no whole-repo call graph is built.
//
// Output (stdout): a single JSON object the pipeline can consume.
//
//	{
//	  "module": "<dir>",
//	  "analyzed": true,
//	  "roots": <n>,
//	  "results": [ {"symbol":"pgconn.SecretKey","reachable":false,"sites":[...]} ],
//	  "any_reachable": false,
//	  "dep_funcs_reachable": ["github.com/jackc/pgx/v5/pgconn.(*PgError).Error", ...]
//	}
//
// Exit codes: 0 = analysis completed (see JSON), 1 = analysis failed (caller MUST
// treat a failure as "unknown" and fall back to the conservative path — never as
// proof of safety).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/rta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

type symResult struct {
	Symbol               string   `json:"symbol"`
	DirectInModule       bool     `json:"direct_in_module"`        // our code directly calls it → compile/signature-break risk
	DirectSites          []string `json:"direct_sites"`            // file:line in THIS module
	TransitivelyReached  bool     `json:"transitively_reachable"` // reachable via the dep's own internals → behavioral-break exposure
}

type output struct {
	Module            string      `json:"module"`
	Analyzed          bool        `json:"analyzed"`
	Roots             int         `json:"roots"`
	Results           []symResult `json:"results"`
	AnyDirectInModule bool        `json:"any_direct_in_module"`
	AnyTransitive     bool        `json:"any_transitively_reachable"`
	DepFuncsReachable []string    `json:"dep_funcs_reachable"`
	Error             string      `json:"error,omitempty"`
}

func die(mod string, err error) {
	out := output{Module: mod, Analyzed: false, Error: err.Error()}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	os.Exit(1)
}

func main() {
	var (
		moduleDir = flag.String("module", ".", "directory of the bumped Go module (contains go.mod)")
		depPrefix = flag.String("dep", "", "import-path prefix of the bumped dependency, e.g. github.com/jackc/pgx/v5")
		symCSV    = flag.String("symbols", "", "comma-separated changed symbols; bare names (SecretKey) or qualified (pgconn.SecretKey)")
		withTests = flag.Bool("tests", true, "include the module's test files as roots")
	)
	flag.Parse()

	if *depPrefix == "" {
		die(*moduleDir, fmt.Errorf("-dep is required"))
	}
	wanted := parseSymbols(*symCSV)

	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedImports | packages.NeedDeps | packages.NeedTypes |
			packages.NeedSyntax | packages.NeedTypesInfo | packages.NeedModule,
		Dir:   *moduleDir,
		Tests: *withTests,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		die(*moduleDir, fmt.Errorf("packages.Load: %w", err))
	}
	if packages.PrintErrors(pkgs) > 0 {
		// Load errors (e.g. a single broken package) are not fatal to reachability;
		// proceed but the caller should weigh this. We still continue.
		fmt.Fprintln(os.Stderr, "[reach] note: some packages had load errors; continuing")
	}
	if len(pkgs) == 0 {
		die(*moduleDir, fmt.Errorf("no packages loaded under %s", *moduleDir))
	}

	prog, ssaPkgs := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()

	// Roots: every function defined in THIS module's own packages. For a library
	// (no main), any exported—or even unexported—function may be an entry point,
	// so seeding all of them is the sound, conservative choice.
	var roots []*ssa.Function
	localPkgPaths := map[string]bool{}
	for _, p := range ssaPkgs {
		if p == nil || p.Pkg == nil {
			continue
		}
		localPkgPaths[p.Pkg.Path()] = true
	}
	for _, p := range ssaPkgs {
		if p == nil {
			continue
		}
		for _, m := range p.Members {
			switch fn := m.(type) {
			case *ssa.Function:
				if isGeneric(fn) {
					continue
				}
				roots = append(roots, fn)
			case *ssa.Type:
				// methods (incl. pointer receivers) are potential entry points
				mset := prog.MethodSets.MethodSet(fn.Type())
				for i := 0; i < mset.Len(); i++ {
					if mfn := prog.MethodValue(mset.At(i)); mfn != nil && !isGeneric(mfn) {
						roots = append(roots, mfn)
					}
				}
				pmset := prog.MethodSets.MethodSet(types.NewPointer(fn.Type()))
				for i := 0; i < pmset.Len(); i++ {
					if mfn := prog.MethodValue(pmset.At(i)); mfn != nil && !isGeneric(mfn) {
						roots = append(roots, mfn)
					}
				}
			}
		}
	}

	res := rta.Analyze(roots, true)

	// Walk the call graph; collect every reachable function that lives in the
	// bumped dependency, and match against the wanted symbol set.
	// Two distinct signals per symbol:
	//   directSites  — an edge whose CALLER is in our module and CALLEE is the symbol
	//                  (a compile/signature break would hit us here).
	//   transitive   — the symbol appears anywhere in the reachable set, i.e. the dep
	//                  reaches it internally on our behalf (behavioral-change exposure).
	directSites := map[string][]string{}
	transitive := map[string]bool{}
	depReachable := map[string]bool{}
	if res.CallGraph != nil {
		callgraph.GraphVisitEdges(res.CallGraph, func(e *callgraph.Edge) error {
			markTransitive(e.Callee.Func, *depPrefix, wanted, depReachable, transitive)
			markTransitive(e.Caller.Func, *depPrefix, wanted, depReachable, transitive)
			markDirect(e, *depPrefix, localPkgPaths, wanted, directSites)
			return nil
		})
	}
	for fn := range res.Reachable {
		markTransitive(fn, *depPrefix, wanted, depReachable, transitive)
	}

	out := output{Module: *moduleDir, Analyzed: true, Roots: len(roots)}
	for _, w := range wanted {
		sites := dedup(directSites[w.key])
		r := symResult{
			Symbol:              w.orig,
			DirectInModule:      len(sites) > 0,
			DirectSites:         sites,
			TransitivelyReached: transitive[w.key],
		}
		if r.DirectInModule {
			out.AnyDirectInModule = true
		}
		if r.TransitivelyReached {
			out.AnyTransitive = true
		}
		out.Results = append(out.Results, r)
	}
	for f := range depReachable {
		out.DepFuncsReachable = append(out.DepFuncsReachable, f)
	}
	sort.Strings(out.DepFuncsReachable)

	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}

type want struct {
	orig string // as supplied
	pkg  string // optional package selector (last path element), "" if bare
	name string // symbol name
	key  string // normalized match key
}

func parseSymbols(csv string) []want {
	var ws []want
	for _, raw := range strings.Split(csv, ",") {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		// strip a receiver form like (*PgConn).SecretKey -> SecretKey, pkg unknown
		name := s
		pkg := ""
		if i := strings.LastIndex(name, "."); i >= 0 {
			pkg = name[:i]
			name = name[i+1:]
		}
		name = strings.Trim(name, "()*")
		pkg = strings.Trim(pkg, "()*")
		// pkg may be "pgconn" or "(*PgConn)" — keep only an identifier-ish selector
		if strings.ContainsAny(pkg, "*()") || pkg == "" {
			pkg = ""
		}
		ws = append(ws, want{orig: s, pkg: pkg, name: name, key: name})
	}
	return ws
}

// markTransitive flags a symbol as reachable-anywhere if fn (a dependency
// function) matches a wanted symbol name. It also records every reachable dep
// function for the evidence dump.
func markTransitive(fn *ssa.Function, depPrefix string, wanted []want,
	depReachable map[string]bool, transitive map[string]bool) {
	if fn == nil || fn.Pkg == nil || fn.Pkg.Pkg == nil {
		return
	}
	if !strings.HasPrefix(fn.Pkg.Pkg.Path(), depPrefix) {
		return
	}
	depReachable[qualified(fn)] = true
	for _, w := range wanted {
		if w.name == fn.Name() {
			transitive[w.key] = true
		}
	}
}

// markDirect records a call site ONLY when the caller is in our own module and
// the callee is a wanted symbol — the precise compile/signature-break signal
// that RTA resolves soundly (including interface and func-value dispatch).
func markDirect(e *callgraph.Edge, depPrefix string, local map[string]bool,
	wanted []want, sites map[string][]string) {
	if e == nil || e.Callee == nil || e.Caller == nil {
		return
	}
	callee, caller := e.Callee.Func, e.Caller.Func
	if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg == nil {
		return
	}
	if !strings.HasPrefix(callee.Pkg.Pkg.Path(), depPrefix) {
		return
	}
	if !local[callerPath(caller)] {
		return
	}
	for _, w := range wanted {
		if w.name != callee.Name() {
			continue
		}
		site := qualified(callee)
		if pos := caller.Prog.Fset.Position(e.Site.Pos()); pos.IsValid() {
			site = fmt.Sprintf("%s:%d", pos.Filename, pos.Line)
		}
		sites[w.key] = append(sites[w.key], site)
	}
}

func callerPath(fn *ssa.Function) string {
	if fn == nil || fn.Pkg == nil || fn.Pkg.Pkg == nil {
		return ""
	}
	return fn.Pkg.Pkg.Path()
}

// isGeneric reports whether fn is an uninstantiated generic template (it has
// type parameters). RTA must not be seeded with such templates — it visits the
// concrete instantiations created under ssa.InstantiateGenerics instead, and
// feeding it a template panics on the leaked type parameter.
func isGeneric(fn *ssa.Function) bool {
	if fn == nil {
		return false
	}
	return fn.TypeParams().Len() > 0
}

func qualified(fn *ssa.Function) string {
	if fn == nil {
		return "?"
	}
	if fn.Pkg != nil && fn.Pkg.Pkg != nil {
		return fn.Pkg.Pkg.Path() + "." + fn.Name()
	}
	return fn.String()
}

func dedup(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
