package zipmt

import (
	"compress/flate"
	"compress/gzip"
	"io"
)

type GZipper struct{}

// Implements compressing the part using GZIP
func (p *GZipper) Shrink(input_bytes *[]byte, out_writer io.Writer) error {
	zw, err := gzip.NewWriterLevel(out_writer, flate.BestCompression)
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

func (p *GZipper) Verify(input io.Reader) error {
	reader, err := gzip.NewReader(input)
	if err != nil {
		return err
	}
	for {
		buf := make([]byte, 4096*10)
		_, err = reader.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
