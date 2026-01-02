package main


import (
	"strings"
	"strconv"
	"bufio"
	"os"
	"fmt"
)

const fileName = ".output/log"
const snapShotFilename = ".output/snapshot"
const walFileName = ".output/wal"

type WAL struct {
	fileName string
	file *os.File


	State map[string]int
}

func NewWAL(fileName string) *WAL {
	file, _ := os.OpenFile(fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	return &WAL{ fileName: fileName, file: file, State: make(map[string]int) }
}

func initWAL() *WAL {
	wal := NewWAL(fileName)

	wal.readFile()
	return wal
}

func (w *WAL) CloseWAL() {
	defer w.file.Close()
}

func (w *WAL) CreateStateSnapShot() {
	file, _ := os.OpenFile(fileName, os.O_CREATE | os.O_WRONLY | os.O_TRUNC, 0644)
	for key, value := range w.State {
		file.WriteString(fmt.Sprintf("%s:%d:%s\n", key, value, "INIT"))
	}
	file.Sync()

	appendFile, _ := os.OpenFile(fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	w.file = appendFile
}

func (w *WAL) updateState(key, method string, value int) {
	switch method {
	case "INCR":
		w.State[key] += value
	case "DECR":
		w.State[key] -= value
	case "INIT":
		w.State[key] = value
	}
}

func (w *WAL) appendLine(key, method string, value int) error {
	content := fmt.Sprintf("%s:%d:%s\n", key, value, method)
	_, err := w.file.WriteString(content)
	w.file.Sync()
	if err != nil {
		return fmt.Errorf("writing to file failed : %v", err)
	}
	w.updateState(key, method, value)
	return nil
}

func parseInstruction(content string) (string, int, string) {
	entries := strings.Split(content, ":")
	if len(entries) == 3 {
		deltaVal, _ := strconv.Atoi(entries[1])
		return entries[0], deltaVal, entries[2]
	}
	return "INVALID", 0, ""
}

func (w *WAL) readFile() {
	file, err := os.OpenFile(fileName, os.O_RDONLY | os.O_CREATE, 0644)
	if err != nil {
		return
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, val, method := parseInstruction(scanner.Text())
		if key == "INVALID" {
			continue
		}
		w.updateState(key, method, val)
	}
}

func main() {
	wal := initWAL()
	// wal.appendLine("key1", "INCR", 12)
	// wal.appendLine("key2", "INCR", 8)
	// wal.appendLine("key3", "INCR", 18)
	//
	// wal.CreateStateSnapShot()
	//
	// wal.appendLine("key5", "INCR", 9)
	// wal.appendLine("key6", "INCR", 13)

	fmt.Println(wal.State)
}
