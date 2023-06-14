package zipmt

import (
	"io"

	"github.com/ulikunitz/xz"
)

type XZZipper struct{}

// Implements compressing the part using XZ
func (p XZZipper) Shrink(input_bytes []byte, out_writer io.Writer) error {
	zw, err := xz.NewWriter(out_writer)
	if err != nil {
		return err
	}
	_, err = zw.Write(input_bytes)
	if err != nil {
		return err
	}
	err = zw.Close()
	return err
}

func (p XZZipper) Verify(input io.Reader) error {
	reader, err := xz.NewReader(input)
	if err != nil {
		err = reader.Verify()
	}
	return err
}
