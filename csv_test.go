package main

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/gocarina/gocsv"
)

type Record struct {
	ID    string `csv:"id"`
	Name  string `csv:"name"`
	Phone string `csv:"phone"`
}

func TestCsv(t *testing.T) {
	//if len(os.Args) != 2 {
	//	fmt.Println("Usage:\nimporter <filename.csv>")
	//	return
	//}
	ch, err := run("test.csv")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	// do something with the records
	for r := range ch {
		fmt.Println(r) // interesting code here
	}

}

func run(fileName string) (chan Record, error) {
	fileHandle, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	//defer fileHandle.Close()

	// make channel
	c := make(chan Record)

	go func() { // start parsing the CSV file
		err = gocsv.UnmarshalToChan(fileHandle, c) // <---- here it is
		if err != nil {
			log.Fatal(err)
		}
	}()

	return c, nil
	//}()

	//return nil
}
