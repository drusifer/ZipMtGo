package main

import (
	"bufio"
	"log"
	"os"

	"github.com/druisfer/zipmt-go/zipmt"
)

func main() {
	in_f, err := os.Open("C:\\Users\\drusi\\Downloads\\android-studio-2022.1.1.20-windows.exe")
	if err != nil {
		log.Fatal("Err opening input file: " + err.Error())
		return
	}
	reader := bufio.NewReader(in_f)

	out_f, err := os.Create("C:\\Users\\drusi\\Downloads\\android-studio-2022.1.1.20-windows.exe.gz")
	if err != nil {
		log.Fatal("Err opening output file: " + err.Error())
	}
	writer := bufio.NewWriter(out_f)
	zipmt.ZipMt(reader, writer)
}
