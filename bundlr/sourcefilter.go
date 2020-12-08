package bundlr

import (
	"os"
	"path/filepath"
)

type SourceFilter interface {
	Filter([]os.FileInfo) []os.FileInfo
}

type filter struct {
	allowList []string
	denyList  []string
}

func NewFilter(allowList []string, denyList []string) SourceFilter {
	return &filter{
		allowList: allowList,
		denyList:  denyList,
	}
}

func (f filter) Filter(files []os.FileInfo) []os.FileInfo {
	results := []os.FileInfo{}
	for _, file := range files {
		if ok, err := f.check(file.Name()); err == nil && ok {
			results = append(results, file)
		}
	}
	return results
}

func (f filter) check(name string) (bool, error) {
	var matched bool = true
	var err error = nil
	if f.allowList != nil {
		for _, allow := range f.allowList {
			matched, err = filepath.Match(allow, name)
			if err != nil {
				return matched, err
			}
			if matched {
				break
			}
		}
	}

	if f.denyList != nil {
		for _, deny := range f.denyList {
			matched, err = filepath.Match(deny, name)
			if err != nil || matched {
				return !matched, err
			}
		}
		matched = !matched
	}
	return matched, err
}
