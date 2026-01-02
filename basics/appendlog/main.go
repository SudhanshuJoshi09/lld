package main


import (
	"os"
	"fmt"
)


const fileName = ".output/log"

func appendLine(content string) error {
	file, err := os.OpenFile(fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	defer file.Close()

	if err != nil {
		return fmt.Errorf("opening the file failed : %v", err)
	}

	_, err = file.WriteString(content + "\n")
	if err != nil {
		return fmt.Errorf("writing to file failed : %v", err)
	}

	return nil
}


func readFile(fileName string) (string, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		return "", fmt.Errorf("Failed to read file : %v", err)
	}
	return string(content), nil
}

func main() {
	// content := "testing second time"
	// appendLine(content)
	//

	newContent, _ := readFile(fileName)
	fmt.Println(newContent)
}
