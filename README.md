# Bundlr

[![Build status](https://travis-ci.com/popbones/bundlr-go.svg)](https://travis-ci.com/popbones/bundlr-go.svg)
[![Code Coverage](https://codecov.io/gh/popbones/bundlr-go/graph/badge.svg)](https://codecov.io/gh/popbones/bundlr-go)
[![Report Card](https://goreportcard.com/badge/github.com/popbones/bundlr-go)](https://goreportcard.com/report/github.com/popbones/bundlr-go)
[![GoDoc](https://godoc.org/github.com/nathany/looper?status.svg)](https://pkg.go.dev/github.com/popbones/bundlr-go/bundlr)

Bundlr is a go package helps the handling of parted file sets.

Bundlr is a go package that helps to handle parted file sets.
A parted file here is a series of files that stored homogenous set of records. The primary motivation is to avoid one large monolithic file for large datasets.

I developed this package because we needed to exchange parquet files with Apache Spark via S3. Being able to split the dataset into multiple files helps the memory footprint of the parquet go package we are using as well as make the parallelised processing easier.

This package only handles the split reading and writing of data. It does not dictate the actual storage backend or the file format.

## Bundle

We call the data Bundlr handles a "bundle". It is essentially a directory with a sub-directory called `data`.

```
foo.bundle/
    |- data/
        |- data_000000.dat
        |- data_000001.dat
        |- ...
```  

Currently, new files are created when a specified number of records has been written to the current file.

The actual format of the file depending on the decoder/encoder configurated.

In the future we may add more information to the structure to facilitate more functionality. For example, a manifest file or additional resource files.

## How to use

Check `examples/parquet-on-s3`.
