package catalog

import (
	"sort"
)

func pathsOf(files []FileMeta) []string {
	out := make([]string, len(files))

	for i, f := range files {
		out[i] = f.Path
	}

	return out
}

func sameFileSet(a []FileMeta, b []FileMeta) bool {
	if len(a) != len(b) {
		return false
	}

	sa := pathsOf(a)
	sb := pathsOf(b)

	sort.Strings(sa)
	sort.Strings(sb)

	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}

	return true
}

func diffMeta(all []FileMeta, remove []string) []FileMeta {
	rm := make(map[string]bool, len(remove))

	for _, r := range remove {
		rm[r] = true
	}

	out := make([]FileMeta, 0, len(all))

	for _, f := range all {
		if !rm[f.Path] {
			out = append(out, f)
		}
	}

	return out
}
