//go:build ignore

// cover-files — the files the coverage gate cannot see.
//
//	go run scripts/cover-files.go <workspace-root> <merged-profile>
//
// A coverage profile lists the files of the packages that were built into
// some instrumented binary. A package no test binary and no e2e binary
// contains is simply absent: it is not at 0%, it is not in the profile, and
// neither the file floor nor the total of .testcoverage.yml ever counts it.
// That is how an untested package passes a 30%-per-file gate.
//
// This lists every Go file of the workspace (go.work's modules) that has
// executable code, is built by default (its build constraints hold) and is
// not excluded in .testcoverage.yml, yet has no entry in the merged profile —
// and exits 1 when there is one. scripts/cover.sh gate runs it.
package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const modulePrefix = "github.com/tracehubmmp/golang-basics/"

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: cover-files <workspace-root> <merged-profile>")
		os.Exit(2)
	}

	root := os.Args[1]

	missing, checked, err := run(root, os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "cover-files:", err)
		os.Exit(2)
	}

	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "cover-files: %d file(s) with code are in no layer's coverage profile — untested and invisible to the gate:\n", len(missing))

		for _, f := range missing {
			fmt.Fprintln(os.Stderr, "  "+f)
		}

		fmt.Fprintln(os.Stderr, "cover them (a test in the package, or the e2e suite running the binary), or exclude the path in .testcoverage.yml with the reason")
		os.Exit(1)
	}

	fmt.Printf("cover-files: all %d files with code are in the profile\n", checked)
}

func run(root, profile string) (missing []string, checked int, err error) {
	covered, err := profiled(profile)
	if err != nil {
		return nil, 0, err
	}

	excluded, err := excludes(filepath.Join(root, ".testcoverage.yml"))
	if err != nil {
		return nil, 0, err
	}

	modules, err := workspaceModules(filepath.Join(root, "go.work"))
	if err != nil {
		return nil, 0, err
	}

	for _, module := range modules {
		err := filepath.WalkDir(filepath.Join(root, module), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)

			if d.IsDir() {
				// A nested module is walked under its own go.work entry.
				if rel != module && exists(filepath.Join(path, "go.mod")) {
					return filepath.SkipDir
				}

				if name := d.Name(); name == "testdata" || name == "node_modules" || (strings.HasPrefix(name, ".") && rel != module) {
					return filepath.SkipDir
				}

				return nil
			}

			if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") || matches(excluded, rel) {
				return nil
			}

			has, err := hasCode(path)
			if err != nil {
				return err
			}

			if !has {
				return nil
			}

			checked++

			if !covered[rel] {
				missing = append(missing, rel)
			}

			return nil
		})
		if err != nil {
			return nil, 0, err
		}
	}

	sort.Strings(missing)

	return missing, checked, nil
}

// profiled is the set of workspace-relative files with an entry in a text
// coverage profile ("<import path>/<file>:<block> <stmts> <count>").
func profiled(path string) (map[string]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	files := map[string]bool{}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	for sc.Scan() {
		name, _, ok := strings.Cut(sc.Text(), ":")
		if !ok || strings.HasPrefix(name, "mode") {
			continue
		}

		if rel, ok := strings.CutPrefix(name, modulePrefix); ok {
			files[rel] = true
		}
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("%s names no file under %s", path, modulePrefix)
	}

	return files, sc.Err()
}

// excludes reads exclude.paths of .testcoverage.yml: the list items between
// "exclude:" and the next top-level key, each a regular expression up to an
// optional trailing comment.
func excludes(path string) ([]*regexp.Regexp, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var (
		out []*regexp.Regexp
		in  bool
	)

	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "exclude:"):
			in = true
		case in && line != "" && line[0] != ' ' && line[0] != '#':
			in = false
		case in && strings.HasPrefix(strings.TrimSpace(line), "- "):
			expr := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- "))
			if i := strings.Index(expr, " #"); i >= 0 {
				expr = strings.TrimSpace(expr[:i])
			}

			re, err := regexp.Compile(expr)
			if err != nil {
				return nil, fmt.Errorf("%s: exclude %q: %w", path, expr, err)
			}

			out = append(out, re)
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no exclude.paths found — the format this reads has changed", path)
	}

	return out, nil
}

func matches(res []*regexp.Regexp, s string) bool {
	for _, re := range res {
		if re.MatchString(s) {
			return true
		}
	}

	return false
}

// workspaceModules is the `use ( ./a ./b )` block of go.work.
func workspaceModules(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var (
		out []string
		in  bool
	)

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)

		switch {
		case strings.HasPrefix(line, "use ("):
			in = true
		case in && line == ")":
			in = false
		case in && strings.HasPrefix(line, "./"):
			out = append(out, strings.TrimPrefix(line, "./"))
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no modules in a use ( … ) block", path)
	}

	return out, nil
}

// hasCode reports whether the file is part of a default build and declares a
// function with at least one statement — what the coverage tool instruments.
func hasCode(path string) (bool, error) {
	if ok, err := build.Default.MatchFile(filepath.Dir(path), filepath.Base(path)); err != nil || !ok {
		return false, err
	}

	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		return false, err
	}

	found := false

	ast.Inspect(file, func(n ast.Node) bool {
		switch fn := n.(type) {
		case *ast.FuncDecl:
			found = found || (fn.Body != nil && len(fn.Body.List) > 0)
		case *ast.FuncLit:
			found = found || len(fn.Body.List) > 0
		}

		return !found
	})

	return found, nil
}

func exists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}
