package main

import (
	// "bufio"
	"encoding/json"
	"fmt"
	"os"
)

const fileName = ".output/tasks.json"


const (
	TODO TaskStatus = iota
	DONE
)

type TaskStatus int


type transition struct {
	from TaskStatus
	to   TaskStatus
}

var validTransitions = map[transition]struct{}{
	{TODO, DONE}:   {},
}

func validStateTransition(from, to TaskStatus) bool {
	_, ok := validTransitions[transition{from, to}]
	return ok
}


type Task struct {
	Title string `json:"name"`
	Description string `json:"description"`
	TaskStatus TaskStatus `json:"task_status"`
}


func NewTask(title, description string) Task {
	return Task {
		Title: title,
		Description: description,
		TaskStatus: TODO,
	}
}

func put(taskList []Task) error {
	content, err := json.Marshal(taskList)
	if err != nil {
		return err
	}
	err = os.WriteFile(fileName, content, 0667)
	if err != nil {
		return err
	}
	return nil
}

func get() ([]Task, error) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		return []Task{}, err
	}
	var task []Task
	err = json.Unmarshal(content, &task)
	if err != nil {
		return []Task{}, err
	}
	return task, nil
}

func (t *Task) updateStatus(to TaskStatus) error {
	fmt.Println(t.TaskStatus, to)
	if validStateTransition(t.TaskStatus, to) {
		t.TaskStatus = to
		return nil
	} else {
		return fmt.Errorf("failed to update status : invalid status change")
	}
}

func readTask() Task {
	var title string
	var description string
	fmt.Scanln(&title)
	fmt.Scanln(&description)
	return Task {Title: title, Description: description}
}

func main() {
	// Adding precursor condition of loading the list from the memory and then appending items.
	taskList, _ := get()
	err := taskList[0].updateStatus(DONE)
	if err != nil {
		fmt.Println(err)
	}
	err = taskList[0].updateStatus(TODO)
	if err != nil {
		fmt.Println(err)
	}
	put(taskList)

	actualTaskList, _ := get()

	fmt.Println(actualTaskList)
}
