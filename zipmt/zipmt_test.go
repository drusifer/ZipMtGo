package zipmt_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/druisfer/zipmt-go/zipmt"
)

func TestZipMt(t *testing.T) {
	t.Fatal("Wrong Answer")
}

func TestCompress(t *testing.T) {
	expected := "Data that was written!!"
	part := zipmt.ZipPart{
		Inbuf: []byte(expected),
		In_sz: len(expected),
	}
	comp := zipmt.GZipper{}

	err := zipmt.CompressPart(&comp, &part)
	if err != nil {
		t.Fatalf("CompressPart got error: %v", err)
	}
	if part.Out_sz <= 0 {
		t.Fatalf("Got an invalid size of %d", part.Out_sz)
	}

	reader := bytes.NewReader(part.Outbuf[:part.Out_sz])
	err = comp.Verify(reader)
	if err != nil {
		t.Fatalf("CompressPart verificaiton failed error: %v", err)
	}
}

func TestReadChunk(t *testing.T) {
	expected := "Data to read!!"
	reader := bufio.NewReader(bytes.NewReader([]byte(expected)))
	part, err := zipmt.ReadChunk(reader, 0, 1024)
	if err != nil {
		t.Fatalf("ReadChunk got error: %v", err)
	}
	if part.In_sz != len(expected) {
		t.Fatalf("Wrong size read.  expected %d got %d", len(expected), part.In_sz)
	}

	data_read := string(part.Inbuf[:part.In_sz])
	if data_read != expected {
		t.Fatalf("Didn't get the data expected: [%s], got: [%s]", expected, data_read)
	}
}

func TestWriteChunk(t *testing.T) {
	expected := "Data that was written!!"
	part := zipmt.ZipPart{
		Outbuf: []byte(expected),
		Out_sz: len(expected),
	}
	output := new(bytes.Buffer)
	writer := bufio.NewWriter(output)
	err := zipmt.WriteChunk(writer, &part)
	writer.Flush()
	if err != nil {
		t.Fatalf("WriteChunk got error: %v", err)
	}
	if part.Out_sz != len(output.Bytes()) {
		t.Fatalf("Wrong size written.  expected %d got %d", len(output.Bytes()), part.Out_sz)
	}

	data_written := string(output.Bytes()[:part.Out_sz])
	if data_written != expected {
		t.Fatalf("Didn't get the data expected: [%s], got: [%s]", expected, data_written)
	}
}
