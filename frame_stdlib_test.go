package nwws

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// nwws depends on the standard library only: every Go file in the module
// imports only standard-library packages and the module's own, and go.mod
// requires nothing.
func TestImportsOnlyStandardLibrary(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	var module string
	for line := range strings.Lines(string(mod)) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "module":
			if len(fields) == 2 {
				module = strings.Trim(fields[1], `"`)
			}
		case "require":
			t.Errorf("go.mod requires a module: %s", strings.TrimSpace(line))
		}
	}
	if module == "" {
		t.Fatal("go.mod has no module directive")
	}

	seen := map[string]bool{}
	err = filepath.WalkDir(".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// The go command ignores these directories.
			if base := d.Name(); name != "." && (base == "testdata" || strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		seen[name] = true
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range f.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			if isStandard(path) || path == module || strings.HasPrefix(path, module+"/") {
				continue
			}
			t.Errorf("%s imports %s, outside the standard library", name, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !seen["frame.go"] || !seen["frame_stdlib_test.go"] {
		t.Fatalf("the walk missed the package's own files: saw %d files", len(seen))
	}
}

// isStandard reports whether an import path belongs to the standard library,
// whose first path element has no dot (cmd/go's rule for module paths).
func isStandard(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}
