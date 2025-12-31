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
	taskList := make([]Task, 0)
	task := Task{Title: "Something", Description: "First ever task"}
	task2 := Task{Title: "Something2", Description: "Second ever task"}

	taskList = append(taskList, task)
	put(taskList)

	actualTaskList := get()
	fmt.Println(actualTaskList)

	taskList = append(taskList, task2)
	put(taskList)

	actualTaskList = get()
	fmt.Println(actualTaskList)
}
