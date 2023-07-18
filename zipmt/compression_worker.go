package zipmt

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
)

func newCompressorForAlgoName(algo_name AlgoName) Compressor {
	var comp Compressor
	switch algo_name {
	case XZ:
		comp = &XZZipper{}
	case BZ2:
		comp = &BZ2Zipper{}
	case GZ:
		comp = &GZipper{}
	default:
		log.Printf("defaulting algo_name to xz: %s", algo_name)
		comp = &XZZipper{}

	}
	return comp
}

func compressPart(comp Compressor, part *ZipPart) error {
	// make the new buffer to write compressed output from this part
	var err error
	out_buf := bytes.NewBuffer(make([]byte, 0, part.In_sz*2)) // make it bigger in case the data inflates

	// use CountedWriter wraper around out_buf to keep track of how many bytes come out of the compression algo
	writer := CountedWriter{
		Writer: out_buf,
	}
	err = comp.Shrink(part.Inbuf[:part.In_sz], &writer)
	log.Printf("CompressionWorker shrunk part %d from %d to %d bytes",
		part.Num, part.In_sz, writer.Count)

	part.Outbuf = out_buf.Bytes()[:writer.Count]
	part.Out_sz = writer.Count
	return err
}

func compressionWorker(zw *ZipWriter, algo_name AlgoName, jobs chan *ZipPart, results chan *ZipPart) {
	// construct the Compressor
	comp := newCompressorForAlgoName(algo_name)
	var compressor_error error
	for compressor_error == nil {
		part := <-jobs
		if part.In_sz > 0 {
			log.Printf("CompressionWorker got part %d, iseof:%t", part.Num, part.IsEOF)
			err := compressPart(comp, part)

			if err != nil {
				compressor_error := fmt.Errorf("error from compressor Shrink %s: %s. Error: %v",
					algo_name, reflect.TypeOf(comp), err)
				zw.err.Store(compressor_error)
			}
		}
		if part.In_sz > 0 || part.IsEOF {
			results <- part
		}
		if part.IsEOF {
			log.Printf("CompressionWorker got EOF in %d", part.Num)
			break // terminates CompressionWorker
		}
	}
}
