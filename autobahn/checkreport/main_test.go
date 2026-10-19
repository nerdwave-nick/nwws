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
		"7.1.6": {"behavior": "INFORMATIONAL", "behaviorClose": "INFORMATIONAL"},
		"13.3.1": {"behavior": "UNIMPLEMENTED", "behaviorClose": "OK"}
	}}`
	// documented explains each case for the behaviour of its ID prefix:
	// "N:6.4.3" for NON-STRICT, "U:13.3.1" for UNIMPLEMENTED.
	documented := func(ids ...string) func(string, string) bool {
		return func(behavior, id string) bool {
			for _, d := range ids {
				if d == behavior[:1]+":"+id {
					return true
				}
			}
			return false
		}
	}

	t.Run("passing", func(t *testing.T) {
		r, err := check(strings.NewReader(passing), "nwws", documented("N:6.4.3", "U:13.3.1"))
		if err != nil {
			t.Fatalf("check = %v, want nil", err)
		}
		want := map[string]int{"OK": 2, "NON-STRICT": 1, "INFORMATIONAL": 1, "UNIMPLEMENTED": 1}
		if len(r.counts) != len(want) {
			t.Errorf("counts = %v, want %v", r.counts, want)
		}
		for b, n := range want {
			if r.counts[b] != n {
				t.Errorf("counts[%s] = %d, want %d", b, r.counts[b], n)
			}
		}
		if r.total != 5 {
			t.Errorf("total = %d, want 5", r.total)
		}
	})

	failing := []struct {
		name, report, agent string
		documented          func(string, string) bool
		want                string // in the error
	}{
		{"failed case", `{"nwws": {"1.1.1": {"behavior": "OK", "behaviorClose": "OK"},
			"5.1": {"behavior": "FAILED", "behaviorClose": "OK"}}}`, "nwws", documented(), "5.1"},
		{"wrong close", `{"nwws": {"7.1.1": {"behavior": "OK", "behaviorClose": "FAILED"}}}`,
			"nwws", documented(), "7.1.1"},
		{"unimplemented", `{"nwws": {"12.1.1": {"behavior": "UNIMPLEMENTED", "behaviorClose": "UNIMPLEMENTED"}}}`,
			"nwws", documented(), "12.1.1"},
		{"undocumented non-strict", passing, "nwws", documented("U:13.3.1"), "6.4.3"},
		{"undocumented unimplemented", passing, "nwws", documented("N:6.4.3"), "13.3.1"},
		{"non-strict listed as unimplemented", passing, "nwws", documented("U:6.4.3", "U:13.3.1"), "6.4.3"},
		{"unimplemented listed as non-strict", passing, "nwws", documented("N:6.4.3", "N:13.3.1"), "13.3.1"},
		{"unimplemented close", `{"nwws": {"12.1.1": {"behavior": "OK", "behaviorClose": "UNIMPLEMENTED"}}}`,
			"nwws", documented(), "12.1.1"},
		{"missing agent", passing, "other", documented("N:6.4.3", "U:13.3.1"), "other"},
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

## UNIMPLEMENTED cases

| 13.3.1, 13.3.2 | reason |

## INFORMATIONAL cases

7.1.6
`)
	for _, tc := range []struct {
		behavior, id string
		want         bool
	}{
		{"NON-STRICT", "6.4.3", true}, {"NON-STRICT", "6.4.4", true}, {"NON-STRICT", "5.15", true},
		{"NON-STRICT", "3.2", true}, {"NON-STRICT", "6.4.1", false}, {"NON-STRICT", "6.4", false},
		{"NON-STRICT", "1.1.1", false}, {"NON-STRICT", "7.1.6", false}, {"NON-STRICT", "13.3.1", false},
		{"UNIMPLEMENTED", "13.3.1", true}, {"UNIMPLEMENTED", "13.3.2", true}, {"UNIMPLEMENTED", "6.4.3", false},
		{"UNIMPLEMENTED", "13.3.3", false},
	} {
		if got := doc(tc.behavior, tc.id); got != tc.want {
			t.Errorf("documented(%s, %q) = %v, want %v", tc.behavior, tc.id, got, tc.want)
		}
	}
}
