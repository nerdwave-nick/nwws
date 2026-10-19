// Command checkreport checks Autobahn report summaries (index.json) strictly:
// every case of the agent must be OK, or INFORMATIONAL where the suite
// defines no stricter outcome, in both its behaviour and its close
// behaviour. NON-STRICT, UNIMPLEMENTED and every other outcome fail. It
// prints the number of cases per behaviour.
//
// Usage:
//
//	checkreport [-agent nwws] index.json...
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
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: checkreport [-agent nwws] index.json...")
		os.Exit(2)
	}
	failed := false
	for _, name := range flag.Args() {
		if err := checkFile(name, *agent); err != nil {
			fmt.Fprintf(os.Stderr, "%s: FAIL\n%v\n", name, err)
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

// passing lists the behaviours of a passed case. The close behaviour is
// held to the same standard.
var passing = map[string]bool{"OK": true, "INFORMATIONAL": true}

func checkFile(name, agent string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	r, err := check(f, agent)
	if r.total > 0 {
		fmt.Printf("%s: %d cases:", name, r.total)
		for _, b := range slices.Sorted(maps.Keys(r.counts)) {
			fmt.Printf(" %s %d", b, r.counts[b])
		}
		fmt.Println()
	}
	return err
}

// check reads an index.json from r and checks the cases of agent.
func check(r io.Reader, agent string) (result, error) {
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
