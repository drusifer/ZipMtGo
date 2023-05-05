package zipmt

import (
	"bufio"
	"bytes"
	"log"

	"github.com/larzconwell/bzip2"
)

type BZ2Zipper struct{}

// Implements compressing the part using GZIP
func (p *BZ2Zipper) Shrink(part *ZipPart) (*ZipPart, error) {
	out_bufz := part.in_sz + int(float64(part.in_sz)*0.50) // make it a little bigger in case shrink needs extra room
	out_buf := make([]byte, out_bufz)
	zw, err := bzip2.NewWriterLevel(bufio.NewWriter(bytes.NewBuffer(out_buf)), bzip2.BestCompression)
	if err != nil {
		log.Fatal("BZ2ZIP Error with new writer: " + err.Error())
	}
	bytes_written, err := zw.Write(part.inbuf)
	zw.Close()
	log.Printf("Compression complete. %d bytes written. err: %s", bytes_written, err)
	if err != nil {
		log.Fatal("BZ2ZIP Error: " + err.Error())
	}
	if bytes_written > out_bufz {
		log.Fatalf("Buffer overflow: bytes_written:%d, outbufz:%d", bytes_written, out_bufz)
	}
	compressed_part := ZipPart{
		outbuf: out_buf,
		out_sz: bytes_written,
		num:    part.num,
		isEOF:  part.isEOF,
	}
	return &compressed_part, err
}
