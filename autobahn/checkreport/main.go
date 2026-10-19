// Command checkreport checks an Autobahn report summary (index.json): every
// case of the agent must have passed, and each NON-STRICT or UNIMPLEMENTED
// case must be explained in the README. It prints the number of cases per
// behaviour.
//
// Usage:
//
//	checkreport [-agent nwws] [-readme autobahn/README.md] index.json...
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
	readme := flag.String("readme", "", "Markdown file whose \"## NON-STRICT cases\" and \"## UNIMPLEMENTED cases\" sections must mention every such case")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: checkreport [-agent nwws] [-readme file] index.json...")
		os.Exit(2)
	}
	documented := func(string, string) bool { return true }
	if *readme != "" {
		b, err := os.ReadFile(*readme)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		documented = documentedIn(string(b))
	}
	failed := false
	for _, name := range flag.Args() {
		if err := checkFile(name, *agent, documented); err != nil {
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

// passing lists the behaviours of a passed case. The closing behaviour is
// held to the same standard.
var passing = map[string]bool{"OK": true, "NON-STRICT": true, "INFORMATIONAL": true}

// explained lists the behaviours that pass only when the README explains
// the case in the section for that behaviour.
var explained = map[string]bool{"NON-STRICT": true, "UNIMPLEMENTED": true}

func checkFile(name, agent string, documented func(behavior, id string) bool) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	r, err := check(f, agent, documented)
	if r.total > 0 {
		fmt.Printf("%s: %d cases:", name, r.total)
		for _, b := range slices.Sorted(maps.Keys(r.counts)) {
			fmt.Printf(" %s %d", b, r.counts[b])
		}
		fmt.Println()
	}
	return err
}

// check reads an index.json from r and checks the cases of agent. A
// NON-STRICT or UNIMPLEMENTED case passes only if documented reports it as
// explained for that behaviour.
func check(r io.Reader, agent string, documented func(behavior, id string) bool) (result, error) {
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
		if err := checkCase(id, c, documented); err != nil {
			problems = append(problems, err)
		}
	}
	return res, errors.Join(problems...)
}

// checkCase checks the behaviour and the close behaviour of one case.
func checkCase(id string, c caseResult, documented func(behavior, id string) bool) error {
	for _, b := range []string{c.Behavior, c.BehaviorClose} {
		if explained[b] && !documented(b, id) {
			return fmt.Errorf("case %s: %s without a reason in the README", id, b)
		}
		if !passing[b] && !explained[b] {
			return fmt.Errorf("case %s: behavior %s, close behavior %s", id, c.Behavior, c.BehaviorClose)
		}
	}
	return nil
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

// documentedIn returns whether a case ID appears as a whole word in the
// Markdown section of a behaviour, "## NON-STRICT cases" for NON-STRICT,
// up to the next heading.
func documentedIn(text string) func(behavior, id string) bool {
	words := map[string]map[string]bool{} // case IDs by section heading
	var heading string
	for line := range strings.Lines(text) {
		if strings.HasPrefix(line, "#") {
			heading = strings.TrimSpace(line)
			continue
		}
		for _, w := range strings.FieldsFunc(line, func(r rune) bool { return r != '.' && (r < '0' || r > '9') }) {
			if words[heading] == nil {
				words[heading] = map[string]bool{}
			}
			words[heading][strings.TrimRight(w, ".")] = true
		}
	}
	return func(behavior, id string) bool { return words["## "+behavior+" cases"][id] }
}
