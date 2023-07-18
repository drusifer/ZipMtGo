package zipmt

import (
	"io"
	"log"
	"math"
	"runtime"
	"sync/atomic"
)

type ZipWriter struct {
	output_writer io.Writer
	pool_size     int
	chunk_size    int
	current_chunk []byte
	jobs          chan *ZipPart
	results       chan *ZipPart
	eof           chan bool
	part_number   int
	algo_name     AlgoName
	err           atomic.Value
}

func (w *ZipWriter) Write(data []byte) (n int, err error) {
	// make sure the writer is still healthy
	async_error := w.err.Load().(error)
	if async_error != nil {
		return 0, async_error
	}
	length := len(data)
	chunks := (length / w.chunk_size) + 1
	for i := 0; i < chunks; i++ {
		start := i * w.chunk_size
		end := int(math.Min(float64(length), float64((i+1)*w.chunk_size)))
		chunkz := end - start
		if chunkz > 0 {
			chunk := make([]byte, chunkz)
			copy(data[start:start+chunkz], chunk)

			//create a ZipPart using the written data and send it to the queue
			part := ZipPart{
				Inbuf: chunk,
				In_sz: chunkz,
				Num:   w.part_number,
			}
			w.part_number++
			w.jobs <- &part
		}
	}
	return len(data), nil
}

func (w *ZipWriter) Close() (err error) {
	// send EOF and wait for pools to empty
	part := ZipPart{
		IsEOF: true,
	}
	w.jobs <- &part

	// block for done
	<-w.eof
	// done reciecved

	// return any error that occured.
	return w.err.Load().(error)
}

func NewZipWriter(output io.Writer, algo AlgoName) (w ZipWriter) {
	zw := ZipWriter{
		output_writer: output,
		algo_name:     algo,
	}
	zw.start()
	return zw
}

func (w *ZipWriter) start() {
	// set up workers for reading data provided by calls to Write
	w.pool_size = runtime.NumCPU()
	w.chunk_size = 1024 * 1024 * 4 //4mb chunks
	w.current_chunk = make([]byte, w.chunk_size)
	log.Printf("Running ZipMt with pool_size:%d and chunk_size:%d", w.pool_size, w.chunk_size)
	//initialize the worker pool
	w.jobs = make(chan *ZipPart, w.pool_size)
	w.results = make(chan *ZipPart, w.pool_size)
	w.eof = make(chan bool)

	// start the compression workers
	i := 0
	for i < w.pool_size {

		go compressionWorker(w, w.algo_name, w.jobs, w.results)
		i++
	}
	// write the results out until done
	go writeWorker(w, w.output_writer, w.results, w.eof)
}
