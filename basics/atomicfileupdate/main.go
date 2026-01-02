package main

import (
	"fmt"
	"os"
	"sync"
)

const oldVersionTargetFileName = ".output/target_v%v.txt"
const targetFileName = ".output/target.txt"
const targetSwapFileName = ".output/target_swap.txt"


type FileRecord struct {
	commitID int
	fileName string
	file 	 *os.File

	sync.Mutex
}


func NewFileRecord(fileName string, openFD bool, commitID int) *FileRecord {
	var file *os.File
	if openFD {
		file, _ = os.OpenFile(fileName, os.O_CREATE | os.O_WRONLY | os.O_TRUNC, 0644)
	}
	return &FileRecord {
		commitID: commitID,
		fileName: fileName,
		file: file,
	}
}

func (fr *FileRecord) write(content string) {
	fr.Lock()
	defer fr.Unlock()
	fr.file.WriteString(content)
	fr.commitID += 1
}

type AtomicReplace struct {
	OldFileRecord *FileRecord
	NewFileRecord *FileRecord

	sync.Mutex
}


func NewAtomicReplace() *AtomicReplace {
	return &AtomicReplace{
		OldFileRecord: NewFileRecord(targetFileName, false, 0),
		NewFileRecord: NewFileRecord(targetSwapFileName, true, 0),
	}
}


func (ar *AtomicReplace) Write(content string) {
	ar.NewFileRecord.write(content)
	ar.Replace()
}

func (ar *AtomicReplace) Replace() {
	ar.Lock()

	if ar.OldFileRecord.commitID == ar.NewFileRecord.commitID {
		return
	}

	newFD := ar.NewFileRecord.file

	defer ar.Unlock()
	defer newFD.Close()

	err := ar.NewFileRecord.file.Sync()
	if err != nil {
		fmt.Println("Failed to flush the file ", err)
		return
	}

	os.Rename(targetSwapFileName, targetFileName)

	ar.OldFileRecord = NewFileRecord(targetFileName, false, ar.NewFileRecord.commitID)
	ar.NewFileRecord = NewFileRecord(targetSwapFileName, true, ar.NewFileRecord.commitID)
}

func main() {
	ar := NewAtomicReplace()
	ar.Write("Sudhanshu Joshi is great")
	ar.Replace()
}
