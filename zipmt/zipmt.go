package zipmt

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"reflect"
	"runtime"
	"time"

	"github.com/emirpasic/gods/sets/treeset"
)

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

func ReadWorker(input *bufio.Reader, jobs chan *ZipPart, pool_size int, chunk_size int) {
	// take an input stream
	part_num := 0
	err := io.EOF
	for {
		// chop off chunks of input into numbered parts
		part, err := ReadChunk(input, part_num, chunk_size)
		log.Printf("read %d from input at part %d\n", part.In_sz, part.Num)
		// send each part into a workerpool of compression workers
		jobs <- part
		part_num++
		if err != nil {
			break
		}
	}

	if err == io.EOF {
		log.Printf("Read Work got eof and is done.")
		i := 0
		for i < pool_size-1 {
			i++
			jobs <- &ZipPart{Num: part_num + i, IsEOF: true} // make sure all the workers get the EOF message
		}
	} else {
		log.Fatal("Read Worker IO Error: " + err.Error())
	}
}

func NewCompressorForAlgoName(algo_name string) Compressor {
	var comp Compressor
	switch algo_name {
	case "xz":
		comp = &XZZipper{}
	case "bz2":
		comp = &BZ2Zipper{}
	case "gz":
		comp = &GZipper{}
	default:
		log.Printf("defaulting algo_name to xz: %s", algo_name)
		comp = &XZZipper{}

	}
	return comp
}
func TestFile(algo_name string, reader io.Reader) error {
	comp := NewCompressorForAlgoName(algo_name)
	return comp.Verify(reader)
}

func CompressPart(comp Compressor, part *ZipPart) error {

	// make the new buffer to write compressed output to
	var err error
	if part.In_sz > 0 {
		out_buf := bytes.NewBuffer(make([]byte, 0, part.In_sz*2)) // make it bigger in case the data inflates
		writer := CountedWriter{
			Writer: *bufio.NewWriter(out_buf),
		}
		err = comp.Shrink(part.Inbuf[:part.In_sz], &writer)
		writer.Writer.Flush()
		log.Printf("CompressionWorker shrunk part %d from %d to %d bytes",
			part.Num, part.In_sz, writer.Count)

		part.Outbuf = out_buf.Bytes()[:writer.Count]
		part.Out_sz = writer.Count
	}
	return err

}

func CompressionWorker(algo_name string, jobs chan *ZipPart, results chan *ZipPart) {
	// construct the Compressor
	comp := NewCompressorForAlgoName(algo_name)
	for {
		part := <-jobs
		log.Printf("CompressionWorker got part %d, iseof:%t", part.Num, part.IsEOF)
		err := CompressPart(comp, part)
		if err != nil {
			log.Fatalf("Error from compressor Shrink %s: %s",
				reflect.TypeOf(comp), err)
		}
		results <- part

		if part.IsEOF {
			log.Printf("CompressionWorker got EOF in %d", part.Num)
			break // terminats CompressionWorker
		}
	}
}

func compareParts(p1, p2 interface{}) int {
	zp1 := p1.(*ZipPart)
	zp2 := p2.(*ZipPart)
	return zp1.Num - zp2.Num
}

// parts come out of order since the compression time varies so make sure we're
func getNextPart(part_num int, results chan *ZipPart, pending_parts *treeset.Set) *ZipPart {
	for {
		// first check to see if we have the expedt part already
		if pending_parts.Size() > 0 {
			itr := pending_parts.Iterator()
			itr.First()
			next_part := itr.Value().(*ZipPart)
			log.Printf("Lowest Part number is %d", next_part.Num)
			if next_part.Num == part_num {
				log.Printf("Retrieved next part %d from pending", next_part.Num)
				pending_parts.Remove(next_part)
				return next_part
			}
		}
		// otherwise wait for new result to arrive and either return it or add it to pending
		part := <-results
		log.Printf("GetNext part got result for part num: %d expecting %d", part.Num, part_num)
		if part.Num == part_num {
			return part
		}
		log.Printf("Out of order part %d. adding to pending_parts (%d)", part.Num, pending_parts.Size())
		pending_parts.Add(part)
	}
}

func WriteChunk(output *bufio.Writer, part *ZipPart) error {
	var err error
	var n int
	if part.Out_sz > 0 {
		n, err = output.Write(part.Outbuf[:part.Out_sz])
		output.Flush()
		if n != part.Out_sz {
			err = fmt.Errorf("tried to write %d but only wrote %d bytes", part.Out_sz, n)
		}
	}
	return err
}

// Worker for reading results off the results channel and outputting the compressed data into
// the output writer.
func WriteWorker(output *bufio.Writer, results chan *ZipPart) {
	pending_parts := treeset.NewWith(compareParts)
	next_part := 0
	for {
		// get the next part from the queue
		part := getNextPart(next_part, results, pending_parts)
		next_part++
		log.Printf("Write Worker got part %d with %d bytes. isEOF? %t", part.Num, part.Out_sz, part.IsEOF)
		err := WriteChunk(output, part)
		if err != nil {
			log.Fatal("Write IO Error: " + err.Error())
		}
		if part.IsEOF {
			break
		}
	}
}

// Does the zip thing using multiple workers to compress the data in chunks
func ZipMt(input *bufio.Reader, output *bufio.Writer, algo_name string) {
	pool_size := runtime.NumCPU()
	chunk_size := 1024 * 1024 * 4 //4mb chunks
	started := time.Now()
	log.Printf("Running ZipMt with pool_size:%d and chunk_size:%d", pool_size, chunk_size)
	//initialize the worker pool
	jobs := make(chan *ZipPart, pool_size)
	results := make(chan *ZipPart, pool_size)
	//start the reader worker
	go ReadWorker(input, jobs, pool_size, chunk_size)

	// start the compression workers
	i := 0
	for i < pool_size {

		go CompressionWorker(algo_name, jobs, results)
		i++
	}
	// write the results out until done
	WriteWorker(output, results)
	ended := time.Now()
	log.Printf("ZipMt Complete. Elapsed: %s", ended.Sub(started))
}
