package bundlr

import (
	"fmt"
	"os"
)

type sortableDataFileInfos []os.FileInfo

func (c sortableDataFileInfos) Len() int {
	return len(c)
}

func (c sortableDataFileInfos) Less(i, j int) bool {
	return parsePartIndexFromFileName(c[i].Name()) < parsePartIndexFromFileName(c[j].Name())
}

func (c sortableDataFileInfos) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func parsePartIndexFromFileName(name string) int {
	index, _ := parseDataFileName(name)
	return index
}

func parseDataFileName(name string) (int, string) {
	var partIndex int
	var ext string
	if _, err := fmt.Sscanf(name, DefaultDataFileNameFormat, &partIndex, &ext); err != nil {
		return -1, ""
	}
	return partIndex, ext
}
