package zipmt

import (
	"fmt"
	"io"
	"log"
)

// parts come out of order since the compression time varies so we keep out of order parts around
// untill we need themn.
func getNextPart(part_num int, results chan *ZipPart, pending_parts map[int]*ZipPart) *ZipPart {

	// first check to see if we have the expected part already
	next_part := pending_parts[part_num]
	if next_part != nil {
		log.Printf("Retrieved next part %d from pending", next_part.Num)
		delete(pending_parts, part_num)
		return next_part
	}

	// otherwise wait for new result to arrive and either return it or add it to pending
	for {
		part := <-results
		log.Printf("GetNext part got result for part num: %d expecting %d", part.Num, part_num)
		if part.Num == part_num {
			return part
		}
		log.Printf("Out of order part %d. adding to pending_parts (%d)", part.Num, len(pending_parts))
		pending_parts[part.Num] = part
	}
}

func writeChunk(output io.Writer, part *ZipPart) error {
	var err error
	var n int
	if part.Out_sz > 0 {
		n, err = output.Write(part.Outbuf[:part.Out_sz])
		if n != part.Out_sz {
			err = fmt.Errorf("tried to write %d but only wrote %d bytes", part.Out_sz, n)
		}
	}
	return err
}

// Worker for reading results off the results channel and outputting the compressed data into
// the output writer.
func writeWorker(zw *ZipWriter, output io.Writer, results chan *ZipPart, done chan bool) {

	// keep completed parts in here (ordered by part number)
	pending_parts := make(map[int]*ZipPart)

	// keep track of which part we need to output next
	next_part := 0

	for {
		// wait for the next part from the queue
		part := getNextPart(next_part, results, pending_parts)
		next_part++

		log.Printf("Write Worker got part %d with %d bytes. isEOF? %t", part.Num, part.Out_sz, part.IsEOF)
		err := writeChunk(output, part)
		if err != nil {
			zw.err.Store(err)
			log.Printf("Write IO Error: " + err.Error())
			break
		}
		if part.IsEOF {
			log.Printf("Write worker got EOF")
			break
		}
	}
	done <- true
}
