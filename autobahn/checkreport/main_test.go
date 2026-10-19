package main

import (
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	const passing = `{"nwws": {
		"1.1.1": {"behavior": "OK", "behaviorClose": "OK"},
		"1.1.2": {"behavior": "OK", "behaviorClose": "OK"},
		"6.4.3": {"behavior": "NON-STRICT", "behaviorClose": "OK"},
		"7.1.6": {"behavior": "INFORMATIONAL", "behaviorClose": "INFORMATIONAL"}
	}}`
	documented := func(ids ...string) func(string) bool {
		return func(id string) bool {
			for _, d := range ids {
				if d == id {
					return true
				}
			}
			return false
		}
	}

	t.Run("passing", func(t *testing.T) {
		r, err := check(strings.NewReader(passing), "nwws", documented("6.4.3"))
		if err != nil {
			t.Fatalf("check = %v, want nil", err)
		}
		want := map[string]int{"OK": 2, "NON-STRICT": 1, "INFORMATIONAL": 1}
		if len(r.counts) != len(want) {
			t.Errorf("counts = %v, want %v", r.counts, want)
		}
		for b, n := range want {
			if r.counts[b] != n {
				t.Errorf("counts[%s] = %d, want %d", b, r.counts[b], n)
			}
		}
		if r.total != 4 {
			t.Errorf("total = %d, want 4", r.total)
		}
	})

	failing := []struct {
		name, report, agent string
		documented          func(string) bool
		want                string // in the error
	}{
		{"failed case", `{"nwws": {"1.1.1": {"behavior": "OK", "behaviorClose": "OK"},
			"5.1": {"behavior": "FAILED", "behaviorClose": "OK"}}}`, "nwws", documented(), "5.1"},
		{"wrong close", `{"nwws": {"7.1.1": {"behavior": "OK", "behaviorClose": "FAILED"}}}`,
			"nwws", documented(), "7.1.1"},
		{"unimplemented", `{"nwws": {"12.1.1": {"behavior": "UNIMPLEMENTED", "behaviorClose": "UNIMPLEMENTED"}}}`,
			"nwws", documented(), "12.1.1"},
		{"undocumented non-strict", passing, "nwws", documented(), "6.4.3"},
		{"missing agent", passing, "other", documented("6.4.3"), "other"},
		{"no cases", `{"nwws": {}}`, "nwws", documented(), "no cases"},
		{"invalid JSON", `{"nwws": `, "nwws", documented(), "index"},
	}
	for _, tc := range failing {
		t.Run(tc.name, func(t *testing.T) {
			_, err := check(strings.NewReader(tc.report), tc.agent, tc.documented)
			if err == nil {
				t.Fatalf("check = nil, want an error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("check = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestCaseIDsSortNumerically(t *testing.T) {
	got := sortedIDs(map[string]caseResult{"10.1.1": {}, "9.2.1": {}, "1.1.10": {}, "1.1.2": {}})
	want := []string{"1.1.2", "1.1.10", "9.2.1", "10.1.1"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("sortedIDs = %v, want %v", got, want)
	}
}

func TestDocumentedIn(t *testing.T) {
	doc := documentedIn(`# Autobahn gate

Case 1.1.1 is mentioned outside the section.

## NON-STRICT cases

| 6.4.3, 6.4.4 | reason |
| 5.15 | reason ending in a case 3.2. |

## INFORMATIONAL cases

7.1.6
`)
	for id, want := range map[string]bool{
		"6.4.3": true, "6.4.4": true, "5.15": true, "3.2": true,
		"6.4.1": false, "6.4": false, "1.1.1": false, "7.1.6": false,
	} {
		if got := doc(id); got != want {
			t.Errorf("documented(%q) = %v, want %v", id, got, want)
		}
	}
}
