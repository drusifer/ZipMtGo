package zipmt

import (
	bz2 "compress/bzip2"
	"io"

	"github.com/larzconwell/bzip2"
)

type BZ2Zipper struct{}

func (p *BZ2Zipper) Shrink(input_bytes *[]byte, out_writer io.Writer) error {
	zw, err := bzip2.NewWriterLevel(out_writer, bzip2.BestCompression)
	if err != nil {
		return err
	}
	_, err = zw.Write(*input_bytes)
	if err != nil {
		return err
	}
	err = zw.Close()
	return err
}

func (p *BZ2Zipper) Verify(input io.Reader) error {
	br := bz2.NewReader(input)
	var err error
	for {
		buf := make([]byte, 4096*10)
		_, err = br.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
