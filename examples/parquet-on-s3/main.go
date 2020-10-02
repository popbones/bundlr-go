package main

import (
	"flag"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws/session"
	s3 "github.com/fclairamb/afero-s3"

	"github.com/popbones/bundlr-go/bundlr"
	"github.com/popbones/bundlr-go/codecs/parquet"
)

type Student struct {
	Name string `parquet:"name=name, type=UTF8"`
	Age  int32  `parquet:"name=age, type=INT32"`
}

func wrd(bundle *bundlr.Bundle) {
	numOfRecords := 2 * 10

	bundle = parquet.ConfigBundle(bundle, new(Student))

	// Write
	writer, err := bundle.WriterWithPartSize(2)
	if err != nil {
		panic(err)
	}

	fmt.Println("writing records...")
	for i := 0; i < numOfRecords; i++ {
		st := Student{
			Name: fmt.Sprintf("student%d", i),
			Age:  int32(i % 18),
		}
		if err := writer.Write(st); err != nil {
			panic(err)
		}
		fmt.Printf("%03d: %v\n", i, st)
	}
	fmt.Println("writing finished")
	if err := writer.Close(); err != nil {
		panic(err)
	}

	// Read
	reader, err := bundle.Reader()
	if err != nil {
		panic(err)
	}

	fmt.Println("reading records...")
	count := 0
	for {
		st := make([]Student, 1)
		err := reader.Read(&st)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		s := st[0]
		fmt.Printf("%03d: %v\n", count, s)
	}
	fmt.Println("reading finished")
	if err := reader.Close(); err != nil {
		panic(err)
	}

	fmt.Println("deleting...")
	if err := bundle.Delete(); err != nil {
		panic(err)
	}
}

func main() {

	bucket := flag.String("b", "", "bucket")
	fileName := flag.String("n", "data.bundle", "bundle file name")

	// You create a session
	sess, err := session.NewSession(nil)
	if err != nil {
		panic(err)
	}

	// Initialize the file system
	s3Fs := s3.NewFs(*bucket, sess)

	bundle, err := bundlr.OpenBundle(s3Fs, *fileName)
	if err != nil {
		panic(err)
	}

	wrd(bundle)
}
