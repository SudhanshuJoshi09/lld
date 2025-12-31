package main


import (
	"os"
	"fmt"
	"encoding/json"
)

const fileName = ".output/content.txt"

type Task struct {
	Title string `json:"name"`
	Description string `json:"description"`
}

func put(task Task) {
	content, err := json.Marshal(task)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(fileName, content, 0667)
	if err != nil {
		panic(err)
	}
}

func get() Task {
	content, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	var task Task
	err = json.Unmarshal(content, &task)
	if err != nil {
		panic(err)
	}
	return task
}

func main() {
	task := Task{Title: "Something", Description: "First ever task"}

	put(task)
	newTask := get()
	fmt.Println(newTask)
}
