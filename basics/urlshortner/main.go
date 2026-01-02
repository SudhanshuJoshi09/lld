package main


import (
	"fmt"
	"os"
	"strings"
	"strconv"
)

const fileName = ".output/urls.txt"


func writeMapping(mapping map[int]string) {
	content := ""
	for key, val := range mapping {
		content += fmt.Sprintf("%d-%s\n", key, val)
	}
	err := os.WriteFile(fileName, []byte(content), 644)
	if err != nil {
		fmt.Println(err)
	}
}

func getMapping() map[int]string {
	byteContent, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}

	result := make(map[int]string)

	content := string(byteContent)
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		lineSplit := strings.Split(line, "-")
		if len(lineSplit) < 2 {
			continue
		}
		count, err := strconv.Atoi(lineSplit[0])
		if err != nil {
			panic(err)
		}
		value := lineSplit[1]
		result[count] = value
	}


	return result
}




func main() {
	collection := getMapping()
	fmt.Println("First log :: ", collection)
	url := "https://youtube.com"
	count := 1
	collection[count] = url
	collection[count] = "https://amazon.com"

	fmt.Println("Second log :: ", collection)
	writeMapping(collection)

	collection = getMapping()
	fmt.Println("Third log :: ", collection)

	fmt.Println(collection[0])
	fmt.Println(collection[1])
}
