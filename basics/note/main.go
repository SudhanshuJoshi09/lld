package main

import (
	"fmt"
	"os"
	"io"
)

const fileName = ".output/clipboard.txt"

func writeContent(content string) {
	// Create directory if it doesn't exist
	err := os.MkdirAll(".output", 0755)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err = f.WriteString(content); err != nil {
		panic(err)
	}
}

func readContent() string {
	content, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	return string(content)
}

func main() {
	byteContent, _ := io.ReadAll(os.Stdin)
	content := string(byteContent)
	writeContent(content)

	content = readContent()
	fmt.Println(content)
}

