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
	Done bool `json:"done"`
}

func put(taskList []Task) {
	content, err := json.Marshal(taskList)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(fileName, content, 0667)
	if err != nil {
		panic(err)
	}
}

func get() []Task {
	content, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	var task []Task
	err = json.Unmarshal(content, &task)
	if err != nil {
		panic(err)
	}
	return task
}

func main() {
	// Adding precursor condition of loading the list from the memory and then appending items.
	taskList := get()
	task := Task{Title: "Something3", Description: "Third ever task"}
	taskList = append(taskList, task)

	put(taskList)
	actualTaskList := get()

	actualTaskList[0].Done = true
	put(actualTaskList)

	newActualTaskList := get()

	fmt.Println(newActualTaskList)
}
