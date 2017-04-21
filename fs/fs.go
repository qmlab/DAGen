package fs

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	assending = iota
	descending
)

// ByDate - fileinfo array sorted by date
type ByDate []os.FileInfo

// ByName - fileinfo array sorted by name
type ByName []os.FileInfo

// LoadFilesByTime - load files by last modified time
func LoadFilesByTime(dir string) (files ByDate) {
	files, e := ioutil.ReadDir(dir)
	if e != nil {
		println("Load DIR error:" + e.Error())
	}

	sort.Sort(files)
	return
}

// LoadFilesByName - load files by name
func LoadFilesByName(dir string) (files ByName) {
	files, e := ioutil.ReadDir(dir)
	if e != nil {
		println("Load DIR error:" + e.Error())
	}

	sort.Sort(files)
	return
}

// LoadFilesWithSuffixByTime - load files by last modified time and filter on suffix
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

func (files ByName) Len() int {
	return len(files)
}

func (files ByName) Swap(i, j int) {
	files[i], files[j] = files[j], files[i]
}

func (files ByName) Less(i, j int) bool {
	num1, e1 := strconv.ParseUint(TrimExt(files[i].Name()), 10, 32)
	num2, e2 := strconv.ParseUint(TrimExt(files[j].Name()), 10, 32)
	if e1 != nil {
		log.Fatal(e1)
	}
	if e2 != nil {
		log.Fatal(e2)
	}
	return num1 < num2
}

// TrimExt - trim extension of file name
func TrimExt(filename string) (name string) {
	var extension = filepath.Ext(filename)
	name = filename[0 : len(filename)-len(extension)]
	return
}

// DeleteFiles - delete the files from a dir
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

// DeleteFilesWithSuffix - delete all files with a suffix in a dir
func DeleteFilesWithSuffix(dir string, suffix string) {
	files := LoadFilesWithSuffixByTime(dir, suffix)
	DeleteFiles(dir, files)
}
