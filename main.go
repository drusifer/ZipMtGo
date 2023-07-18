package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/druisfer/zipmt-go/zipmt"
)

// Elapsed: 1m23.2902475s

func main() {
	input_file_ptr := flag.String("input", "", "The input file name to compress.  Default is stdin")
	output_file_ptr := flag.String("out", "", "The output file name to write the compressed output to. Use '-' for sdout. Default is input_file + suffix")
	algo_ptr := flag.String("algo", "xz", "Must be one of [xz, bz2, gz] default is xz")

	test_file := false
	flag.BoolVar(&test_file, "t", false, "Test the input file to make sure it is valid")

	in_f := os.Stdin // default to stdin for input so we can use it in a pipe line
	var err error = nil
	flag.Parse()
	algo := zipmt.GetAlgoForString(*algo_ptr)

	if *input_file_ptr != "" {
		in_f, err = os.Open(*input_file_ptr)
		if err != nil {
			log.Fatalf("Err opening input file: '%s': %s", *input_file_ptr, err.Error())
		}
	}
	reader := bufio.NewReader(in_f)

	if test_file {
		err := zipmt.TestFile(algo, reader)
		if err != nil {
			log.Fatalf("Test Failure on %s %s:", *input_file_ptr, err)
		} else {
			log.Printf("Test Successful")
		}
		return
	}

	out_f := os.Stdout
	if *output_file_ptr == "" {
		if *input_file_ptr != "" {
			//default to file input name + . + algo suffix
			*output_file_ptr = fmt.Sprintf("%s.%s", *input_file_ptr, *algo_ptr)
		}
	}

	if *output_file_ptr != "-" {
		tmp_f, err := os.Create(*output_file_ptr)
		out_f = tmp_f
		if err != nil {
			log.Fatal("Err opening output file: " + err.Error())
		}
	}

	writer := bufio.NewWriter(out_f)
	zipmt.ZipMt(reader, writer, algo)
	writer.Flush()
	out_f.Close()
}
