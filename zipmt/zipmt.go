package zipmt

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"log"

	"github.com/emirpasic/gods/sets/treeset"
)

type compressor interface {
	Shrink(part *ZipPart) (*ZipPart, error)
}

type ZipPart struct {
	inbuf  []byte
	in_sz  int
	outbuf []byte
	out_sz int
	num    int
	isEOF  bool
}

type GZipper struct{}

// Implements compressing the part using GZIP
func (p *GZipper) Shrink(part *ZipPart) (*ZipPart, error) {
	out_buf := make([]byte, part.in_sz*2)
	zw := gzip.NewWriter(bufio.NewWriter(bytes.NewBuffer(out_buf)))
	bytes_written, err := zw.Write(part.inbuf)
	zw.Close()
	if err != nil {
		log.Fatal("GZIP Error: " + err.Error())
	}
	compressed_part := ZipPart{
		outbuf: out_buf,
		out_sz: bytes_written,
		num:    part.num,
		isEOF:  part.isEOF,
	}
	return &compressed_part, err
}

func ReadChunk(input *bufio.Reader, part_num int, chunk_size int) (*ZipPart, error) {
	inbuf := make([]byte, chunk_size)

	bytes_read, err := input.Read(inbuf)

	part := ZipPart{
		inbuf: inbuf,
		in_sz: bytes_read,
		num:   part_num,
		isEOF: (err == io.EOF),
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
		log.Printf("read %d from input at part %d\n", part.in_sz, part.num)
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
			jobs <- &ZipPart{num: part_num + i, isEOF: true} // make sure all the workers get the EOF message
		}
	} else {
		log.Fatal("Read Worker IO Error: " + err.Error())
	}
}

func CompressionWorker(comp compressor, jobs chan *ZipPart, results chan *ZipPart) {
	// construct the compressor
	for {
		part := <-jobs
		log.Printf("CompressionWorker got part %d", part.num)
		if part.in_sz > 0 {
			shrunk_part, err := comp.Shrink(part)
			if err != nil {
				log.Fatal("Compress Worker Error: " + err.Error())
			}
			log.Printf("CompressionWorker shrunk part %d from %d to %d bytes",
				part.num, part.in_sz, shrunk_part.out_sz)
			results <- shrunk_part
		} else if part.isEOF {
			results <- part // 0 size EOF
		}
		if part.isEOF {
			log.Printf("CompressionWorker got EOF in %d", part.num)
			break
		}
	}
}

func compareParts(p1, p2 interface{}) int {
	zp1 := p1.(*ZipPart)
	zp2 := p2.(*ZipPart)
	return zp1.num - zp2.num
}

// parts come out of order since the compression time varies so make sure we're
func getNextPart(part_num int, results chan *ZipPart, pending_parts *treeset.Set) *ZipPart {
	for {
		part := <-results
		log.Printf("GetNext part got result for part num: %d expecting %d", part.num, part_num)
		if part.num == part_num || part.isEOF {
			return part
		}
		log.Printf("Out of order part %d. adding to pending_parts (%d)", part.num, pending_parts.Size())
		pending_parts.Add(part)
		itr := pending_parts.Iterator()
		itr.First()
		next_part := itr.Value().(*ZipPart)
		log.Printf("Lowest Part number is %d", next_part.num)
		if next_part.num == part_num {
			pending_parts.Remove(next_part)
			return next_part
		}
	}
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
		log.Printf("Write Worker got part %d with %d bytes. isEOF? %t", part.num, part.out_sz, part.isEOF)
		if part.out_sz > 0 {
			n, err := output.Write(part.outbuf[:part.out_sz])
			if n != part.out_sz {
				log.Fatalf("Tried to write %d but only wrote %d bytes", part.out_sz, n)
			}
			if err != nil {
				log.Fatal("Write IO Error: " + err.Error())
			}
		}
		if part.isEOF {
			break
		}
	}
}

// Does the zip thing using multiple workers to compress the data in chunks
func ZipMt(input *bufio.Reader, output *bufio.Writer) {
	pool_size := 16
	chunk_size := 1024 * 1024
	//initialize the worker pool
	jobs := make(chan *ZipPart, pool_size)
	results := make(chan *ZipPart, pool_size)
	//start the reader worker
	go ReadWorker(input, jobs, pool_size, chunk_size)

	// start the compression workers
	i := 0
	for i < pool_size {

		go CompressionWorker(&GZipper{}, jobs, results)
		i++
	}
	// write the results out until done
	WriteWorker(output, results)
}
