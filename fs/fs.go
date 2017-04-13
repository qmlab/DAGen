package fs

import (
	"io/ioutil"
	"os"
	"sort"
)

const (
	assending = iota
	descending
)

type ByDate []os.FileInfo

func LoadFilesByTime(dir string) (files ByDate) {
	files, e := ioutil.ReadDir(dir)
	if e != nil {
		println("Load DIR error:" + e.Error())
	}

	sort.Sort(files)
	return
}

func (files ByDate) Len() int {
	return len(files)
}

func (files ByDate) Swap(i, j int) {
	files[i], files[j] = files[j], files[i]
}

func (files ByDate) Less(i, j int) bool {
	return files[i].ModTime().Before(files[j].ModTime())
}
