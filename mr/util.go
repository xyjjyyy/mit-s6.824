package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func CreateDirs(count int) error {
	pattern := "mr-map-worker-%d"
	for i := 0; i < count; i++ {
		dir := fmt.Sprintf(pattern, i)
		os.MkdirAll(dir, os.ModePerm)
	}

	os.MkdirAll("mr-shuffle-worker/", os.ModePerm)
	return nil
}

func RemoveDirs(count int) error {
	pattern := "mr-map-worker-%d"
	for i := 0; i < count; i++ {
		dir := fmt.Sprintf(pattern, i)
		os.RemoveAll(dir)
	}

	os.RemoveAll("mr-shuffle-worker")
	return nil
}

func CreateTmpFile(dir string, fileName string, kva []KeyValue) error {
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		log.Printf("Create dir %s failed\n", dir)
		return err
	}
	file, err := ioutil.TempFile(dir, "")
	if err != nil {
		return err
	}

	for _, kv := range kva {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}

	err = os.Rename(file.Name(), dir+fileName)
	if err != nil {
		return err
	}
	return nil
}

func ReadKVFile(filename string) ([]KeyValue, error) {
	file, _ := os.Open(filename)

	contents, _ := ioutil.ReadAll(file)

	ff := func(r rune) bool { return r == '\n' || r == ' ' }

	// split contents into an array of words.
	words := strings.FieldsFunc(string(contents), ff)

	var kva []KeyValue
	//fmt.Println("len(words):", len(words))
	for i := 0; i < len(words); i += 2 {
		kv := KeyValue{words[i], words[i+1]}
		kva = append(kva, kv)
	}
	//os.Remove(filename)
	return kva, nil
}
