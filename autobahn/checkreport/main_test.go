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
		r, err := check(strings.NewReader(passing), "nwws", 5)
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
		cases               int    // expected
		want                string // in the error
	}{
		{"non-strict", withCase("NON-STRICT", "OK"), "nwws", 2, "6.4.3"},
		{"unimplemented", withCase("UNIMPLEMENTED", "OK"), "nwws", 2, "6.4.3"},
		{"failed", withCase("FAILED", "OK"), "nwws", 2, "6.4.3"},
		{"wrong code", withCase("WRONG CODE", "OK"), "nwws", 2, "6.4.3"},
		{"non-strict close", withCase("OK", "NON-STRICT"), "nwws", 2, "6.4.3"},
		{"unimplemented close", withCase("OK", "UNIMPLEMENTED"), "nwws", 2, "6.4.3"},
		{"failed close", withCase("OK", "FAILED"), "nwws", 2, "6.4.3"},
		{"failed by peer close", withCase("INFORMATIONAL", "FAILED BY CLIENT"), "nwws", 2, "6.4.3"},
		{"missing agent", passing, "other", 5, "other"},
		{"no cases", `{"nwws": {}}`, "nwws", 5, "no cases"},
		{"invalid JSON", `{"nwws": `, "nwws", 5, "index"},
		// A run that lost a case passes every case it has.
		{"missing case", withCase("OK", "OK"), "nwws", 3, "2 cases, want 3"},
		{"extra case", withCase("OK", "OK"), "nwws", 1, "2 cases, want 1"},
	}
	for _, tc := range failing {
		t.Run(tc.name, func(t *testing.T) {
			_, err := check(strings.NewReader(tc.report), tc.agent, tc.cases)
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
		_, err := check(strings.NewReader(report), "nwws", 4)
		for _, want := range []string{"3.2", "12.1.1", "3 cases, want 4"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("check = %v, want an error mentioning %q", err, want)
			}
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

func TestParseReport(t *testing.T) {
	valid := []struct {
		arg  string
		want report
	}{
		{"reports/server/index.json:301", report{"reports/server/index.json", 301}},
		{"index.json:216", report{"index.json", 216}},
		{"dir:with:colons/index.json:1", report{"dir:with:colons/index.json", 1}},
	}
	for _, tc := range valid {
		got, err := parseReport(tc.arg)
		if err != nil || got != tc.want {
			t.Errorf("parseReport(%q) = %+v, %v, want %+v, nil", tc.arg, got, err, tc.want)
		}
	}
	for _, arg := range []string{"index.json", "index.json:", "index.json:x", "index.json:0", "index.json:-1", ":301"} {
		if got, err := parseReport(arg); err == nil {
			t.Errorf("parseReport(%q) = %+v, nil, want an error", arg, got)
		}
	}
}
