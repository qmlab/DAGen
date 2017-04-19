package fs

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strings"
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

func LoadFilesWithSuffixByTime(dir string, suffix string) (files ByDate) {
	all, e := ioutil.ReadDir(dir)
	if e != nil {
		println("Load DIR error:" + e.Error())
	}

	for _, f := range all {
		if strings.HasSuffix(f.Name(), suffix) {
			files = append(files, f)
		}
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

func DeleteFiles(dir string, files []os.FileInfo) {
	for _, f := range files {
		var err = os.Remove(path.Join(dir, f.Name()))
		if err != nil {
			println("Failed to delete", f.Name())
			log.Fatal(err)
		} else {
			println("Deleted file", f.Name())
		}
	}
}

func DeleteFilesWithSuffix(dir string, suffix string) {
	files := LoadFilesWithSuffixByTime(dir, suffix)
	DeleteFiles(dir, files)
}
