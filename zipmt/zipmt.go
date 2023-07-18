package zipmt

import (
	"bufio"
	"io"
	"log"
	"runtime"
	"time"
)

type AlgoName string

const (
	BZ2 AlgoName = "bz2"
	XZ  AlgoName = "xz"
	GZ  AlgoName = "gz"
)

func (s AlgoName) String() string {
	switch s {
	case BZ2:
		return "bz2"
	case XZ:
		return "xz"
	case GZ:
		return "gz"
	default:
		log.Panicf("Invalid AlgoName")
	}
	return ""
}

func GetAlgoForString(s string) AlgoName {
	switch s {
	case "bz2":
		return BZ2
	case "xz":
		return XZ
	case "gz":
		return GZ
	default:
		log.Panicf("Invalid AlgoName: %s", s)
	}
	return ""
}

type Compressor interface {
	// blocking call to compress the data. It should handle the full lifecycle of the
	// underlying implementation: Create a writer that writes to output_writer,
	// 							  Call Write() and Close() to ensure all data is writen out.
	Shrink(input_buf []byte, output_writer io.Writer) error

	Verify(reader io.Reader) error
}

type ZipPart struct {
	Inbuf  []byte
	In_sz  int
	Outbuf []byte
	Out_sz int
	Num    int
	IsEOF  bool
}

type CountedWriter struct {
	bufio.Writer
	Count int
}

// function that keeps track of how many bytes are passed down.
// I use this because the io.Write() implemention returns the number of bytes passed in
// not the number of bytes written out (post compression)
func (w *CountedWriter) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	w.Count += n
	return n, err
}

func ReadChunk(input *bufio.Reader, part_num int, chunk_size int) (*ZipPart, error) {
	inbuf := make([]byte, chunk_size)

	bytes_read, err := input.Read(inbuf)

	part := ZipPart{
		Inbuf: inbuf,
		In_sz: bytes_read,
		Num:   part_num,
		IsEOF: (err == io.EOF),
	}

	return &part, err
}

func ReadWorker(input *bufio.Reader, zw ZipWriter) {
	buf := make([]byte, zw.chunk_size)
	var err error
	n := 0
	for err != io.EOF {
		n, err = input.Read(buf)
		log.Printf("read %d bytes from input", n)
		_, write_err := zw.Write(buf[:n])
		if write_err != nil {
			log.Panicf("Got write error: %v", write_err)
		}
	}

	if err == io.EOF {
		log.Printf("Read Work got eof and is done.")
		close_err := zw.Close()
		if close_err != nil {
			log.Panicf("Error closing zip worker: %v", close_err)
		}
	}
}

func TestFile(algo_name AlgoName, reader io.Reader) error {
	comp := newCompressorForAlgoName(algo_name)
	return comp.Verify(reader)
}

// Does the zip thing using multiple workers to compress the data in chunks
func ZipMt(input *bufio.Reader, output *bufio.Writer, algo_name AlgoName) {
	pool_size := runtime.NumCPU()
	chunk_size := 1024 * 1024 * 4 //4mb chunks
	started := time.Now()
	log.Printf("Running ZipMt with pool_size:%d and chunk_size:%d", pool_size, chunk_size)
	zw := NewZipWriter(output, algo_name)

	// read the input stream and write to the ZipWriter
	// blocks until all input is compressed (async)
	ReadWorker(input, zw)

	ended := time.Now()
	log.Printf("ZipMt Complete. Elapsed: %s", ended.Sub(started))
}
