// Command checkreport checks Autobahn report summaries (index.json) strictly:
// the agent must have exactly the expected number of cases, and every case
// must be OK, or INFORMATIONAL where the suite defines no stricter outcome,
// in both its behaviour and its close behaviour. NON-STRICT, UNIMPLEMENTED
// and every other outcome fail. A missing case fails too, even when every
// case that ran passed. It prints the number of cases per behaviour.
//
// Usage:
//
//	checkreport [-agent nwws] index.json:cases...
//
// Each argument names a report and its expected number of cases, for
// example reports/server/index.json:301.
package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
)

func main() {
	agent := flag.String("agent", "nwws", "agent whose cases to check")
	flag.Parse()
	const usage = "usage: checkreport [-agent nwws] index.json:cases..."
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	reports := make([]report, flag.NArg())
	for i, arg := range flag.Args() {
		r, err := parseReport(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n%s\n", err, usage)
			os.Exit(2)
		}
		reports[i] = r
	}
	failed := false
	for _, r := range reports {
		if err := checkFile(r, *agent); err != nil {
			fmt.Fprintf(os.Stderr, "%s: FAIL\n%v\n", r.path, err)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

// caseResult is the outcome of one case in index.json.
type caseResult struct {
	Behavior      string `json:"behavior"`
	BehaviorClose string `json:"behaviorClose"`
}

// result summarises the cases of one agent.
type result struct {
	total  int
	counts map[string]int // cases per behaviour
}

// report is a report summary to check and its expected number of cases.
type report struct {
	path  string
	cases int
}

// parseReport parses a report argument, path:cases. The path may itself
// contain colons; the count follows the last one.
func parseReport(arg string) (report, error) {
	i := strings.LastIndexByte(arg, ':')
	if i <= 0 {
		return report{}, fmt.Errorf("%q: want index.json:cases", arg)
	}
	n, err := strconv.Atoi(arg[i+1:])
	if err != nil || n <= 0 {
		return report{}, fmt.Errorf("%q: the expected case count must be a positive integer", arg)
	}
	return report{path: arg[:i], cases: n}, nil
}

// passing lists the behaviours of a passed case. The close behaviour is
// held to the same standard.
var passing = map[string]bool{"OK": true, "INFORMATIONAL": true}

func checkFile(rep report, agent string) error {
	f, err := os.Open(rep.path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	r, err := check(f, agent, rep.cases)
	if r.total > 0 {
		fmt.Printf("%s: %d of %d cases:", rep.path, r.total, rep.cases)
		for _, b := range slices.Sorted(maps.Keys(r.counts)) {
			fmt.Printf(" %s %d", b, r.counts[b])
		}
		fmt.Println()
	}
	return err
}

// check reads an index.json from r and checks the cases of agent, of
// which there must be want.
func check(r io.Reader, agent string, want int) (result, error) {
	var index map[string]map[string]caseResult
	if err := json.NewDecoder(r).Decode(&index); err != nil {
		return result{}, fmt.Errorf("reading the index: %w", err)
	}
	cases, ok := index[agent]
	if !ok {
		return result{}, fmt.Errorf("no results for agent %q", agent)
	}
	if len(cases) == 0 {
		return result{}, fmt.Errorf("agent %q has no cases", agent)
	}
	res := result{total: len(cases), counts: map[string]int{}}
	var problems []error
	if res.total != want {
		problems = append(problems, fmt.Errorf("%d cases, want %d", res.total, want))
	}
	for _, id := range sortedIDs(cases) {
		c := cases[id]
		res.counts[c.Behavior]++
		if !passing[c.Behavior] || !passing[c.BehaviorClose] {
			problems = append(problems, fmt.Errorf("case %s: behavior %s, close behavior %s", id, c.Behavior, c.BehaviorClose))
		}
	}
	return res, errors.Join(problems...)
}

// sortedIDs returns the case IDs in numeric order: 1.1.2 before 1.1.10.
func sortedIDs(cases map[string]caseResult) []string {
	return slices.SortedFunc(maps.Keys(cases), func(a, b string) int {
		pa, pb := strings.Split(a, "."), strings.Split(b, ".")
		for i := range min(len(pa), len(pb)) {
			na, _ := strconv.Atoi(pa[i])
			nb, _ := strconv.Atoi(pb[i])
			if c := cmp.Compare(na, nb); c != 0 {
				return c
			}
		}
		return cmp.Compare(len(pa), len(pb))
	})
}
