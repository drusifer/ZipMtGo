package zipmt

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"testing"
)

func TestZipMt(t *testing.T) {
	t.Fatal("Wrong Answer")
}

func TestCompress(t *testing.T) {
	expected := "Data that was written!!"
	part := ZipPart{
		Inbuf: []byte(expected),
		In_sz: len(expected),
	}
	comp := GZipper{}

	err := compressPart(&comp, &part)
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

func TestCompressGz(t *testing.T) {
	expected := "Data that was written!!"
	part := ZipPart{
		Inbuf: []byte(expected),
		In_sz: len(expected),
	}
	comp := &GZipper{}
	CompressorTest(t, &part, comp)
}
func TestCompressXz(t *testing.T) {
	expected := "Data that was written!!"
	part := ZipPart{
		Inbuf: []byte(expected),
		In_sz: len(expected),
	}
	comp := &XZZipper{}
	CompressorTest(t, &part, comp)
}

func TestCompressBZ2(t *testing.T) {
	expected := "Data that was written!!"
	part := ZipPart{
		Inbuf: []byte(expected),
		In_sz: len(expected),
	}
	comp := &BZ2Zipper{}
	CompressorTest(t, &part, comp)

	reader := bzip2.NewReader(bytes.NewReader(part.Outbuf))
	data := make([]byte, len(expected)*2)
	n, err := reader.Read(data)

	if err != nil {
		t.Fatalf("Got Error when reading compressed data: %v", err)
	}

	if n != len(expected) {
		t.Fatalf("incorrect number of bytes decompressed: expect:%d got:%d", len(expected), n)
	}

	if string(data) != expected {
		t.Fatalf("Expected: `%s` got `%s`", expected, string(data))
	}
}

func CompressorTest(t *testing.T, part *ZipPart, comp Compressor) {
	out := bytes.NewBuffer([]byte{})
	err := comp.Shrink(part.Inbuf, out)
	if err != nil {
		t.Fatalf("Got error from Shrink: %v", err)
	}

	part.Outbuf = out.Bytes()
	part.Out_sz = out.Len()

	r := bufio.NewReader(out)
	verr := comp.Verify(r)
	if verr != nil {
		t.Fatalf("Got Verify Error:%v", err)
	}
}

func TestReadChunk(t *testing.T) {
	expected := "Data to read!!"
	reader := bufio.NewReader(bytes.NewReader([]byte(expected)))
	part, err := ReadChunk(reader, 0, 1024)
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
	part := ZipPart{
		Outbuf: []byte(expected),
		Out_sz: len(expected),
	}
	output := new(bytes.Buffer)
	writer := bufio.NewWriter(output)

	err := writeChunk(writer, &part)
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
