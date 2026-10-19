package main

import (
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	const passing = `{"nwws": {
		"1.1.1": {"behavior": "OK", "behaviorClose": "OK"},
		"1.1.2": {"behavior": "OK", "behaviorClose": "OK"},
		"7.1.6": {"behavior": "INFORMATIONAL", "behaviorClose": "INFORMATIONAL"},
		"7.13.1": {"behavior": "INFORMATIONAL", "behaviorClose": "OK"},
		"7.13.2": {"behavior": "OK", "behaviorClose": "INFORMATIONAL"}
	}}`

	t.Run("passing", func(t *testing.T) {
		r, err := check(strings.NewReader(passing), "nwws")
		if err != nil {
			t.Fatalf("check = %v, want nil", err)
		}
		want := map[string]int{"OK": 3, "INFORMATIONAL": 2}
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

	// withCase is a report of a passing case and case 6.4.3 with the given
	// behaviour and close behaviour.
	withCase := func(behavior, closeBehavior string) string {
		return `{"nwws": {"1.1.1": {"behavior": "OK", "behaviorClose": "OK"},
			"6.4.3": {"behavior": "` + behavior + `", "behaviorClose": "` + closeBehavior + `"}}}`
	}
	failing := []struct {
		name, report, agent string
		want                string // in the error
	}{
		{"non-strict", withCase("NON-STRICT", "OK"), "nwws", "6.4.3"},
		{"unimplemented", withCase("UNIMPLEMENTED", "OK"), "nwws", "6.4.3"},
		{"failed", withCase("FAILED", "OK"), "nwws", "6.4.3"},
		{"wrong code", withCase("WRONG CODE", "OK"), "nwws", "6.4.3"},
		{"non-strict close", withCase("OK", "NON-STRICT"), "nwws", "6.4.3"},
		{"unimplemented close", withCase("OK", "UNIMPLEMENTED"), "nwws", "6.4.3"},
		{"failed close", withCase("OK", "FAILED"), "nwws", "6.4.3"},
		{"failed by peer close", withCase("INFORMATIONAL", "FAILED BY CLIENT"), "nwws", "6.4.3"},
		{"missing agent", passing, "other", "other"},
		{"no cases", `{"nwws": {}}`, "nwws", "no cases"},
		{"invalid JSON", `{"nwws": `, "nwws", "index"},
	}
	for _, tc := range failing {
		t.Run(tc.name, func(t *testing.T) {
			_, err := check(strings.NewReader(tc.report), tc.agent)
			if err == nil {
				t.Fatalf("check = nil, want an error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("check = %q, want it to mention %q", err, tc.want)
			}
		})
	}

	t.Run("every failing case is reported", func(t *testing.T) {
		report := `{"nwws": {"3.2": {"behavior": "NON-STRICT", "behaviorClose": "OK"},
			"1.1.1": {"behavior": "OK", "behaviorClose": "OK"},
			"12.1.1": {"behavior": "UNIMPLEMENTED", "behaviorClose": "UNIMPLEMENTED"}}}`
		_, err := check(strings.NewReader(report), "nwws")
		if err == nil || !strings.Contains(err.Error(), "3.2") || !strings.Contains(err.Error(), "12.1.1") {
			t.Fatalf("check = %v, want an error mentioning 3.2 and 12.1.1", err)
		}
	})
}

func TestCaseIDsSortNumerically(t *testing.T) {
	got := sortedIDs(map[string]caseResult{"10.1.1": {}, "9.2.1": {}, "1.1.10": {}, "1.1.2": {}})
	want := []string{"1.1.2", "1.1.10", "9.2.1", "10.1.1"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("sortedIDs = %v, want %v", got, want)
	}
}
