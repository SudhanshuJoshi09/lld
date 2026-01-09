package main


import (
	"bufio"
	"fmt"
	"os"
)


const (
	fileName = "events.log"
)

type EventLog struct {
	history []string
	fileName string
}

func InitEventLog() (*EventLog, error) {
	// Restore state
	history, err := restoreState(fileName)
	if err != nil {
		return nil, err
	}

	// Question: What is the permission 0644
	// user - group - others
	// rwx : rwx : rwx
	// 110 : 010 : 010

	eventLog := NewEventLog(history, fileName)
	return &eventLog, nil
}

func NewEventLog(history []string, fileName string) EventLog {
	return EventLog {
		history: history,
		fileName: fileName,
	}
}

/* READ STATE */
func restoreState(fileName string) ([]string, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY | os.O_CREATE, 0644)
	if err != nil {
		return []string{}, err
	}

	scanner := bufio.NewScanner(file)
	result := make([]string, 0)

	for scanner.Scan() {
		line := scanner.Text()
		result = append(result, line)
	}

	return result, nil
}

/* WRITE LOG */
func (e *EventLog) writeState() error {
	file, err := os.OpenFile(e.fileName, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	content := ""
	for _, event := range e.history {
		content += fmt.Sprintf("%s\n", event)
	}

	_, err = file.Write([]byte(content))
	if err != nil {
		return err
	}

	// fsync ?? (No, not mandatory this is not WAL (Write ahead log)
	return nil
}



/* APPEND LOG */
func (e *EventLog) AppendLog(event string) error {
	e.history = append(e.history, event)
	err := e.writeState()
	if err != nil {
		return err
	}
	return nil
}


func main() {
	el, err := InitEventLog()

	if err != nil {
		fmt.Println("You done messed up jqaualine")
		fmt.Println(err)
		return
	}
	//
	// err = el.AppendLog("event - 01")
	// err = el.AppendLog("event - 03")
	// err = el.AppendLog("event - 02")
	fmt.Println(el.history)
}
