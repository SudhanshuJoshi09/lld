package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

const (
	dirName  = ".output"
	fileName = "operations.json"
)

type Dedup struct {
	ops   map[string]int
	state int
	file  *os.File
}

/* --------------------- INITIALIZATION ----------------------- */

func InitDedup() (*Dedup, error) {
	if err := os.MkdirAll(dirName, 0744); err != nil {
		return nil, err
	}

	filePath := path.Join(dirName, fileName)

	file, err := os.OpenFile(
		filePath,
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}

	d := &Dedup{
		ops:  make(map[string]int),
		file: file,
	}

	if err := d.RestoreState(); err != nil {
		file.Close()
		return nil, err
	}

	return d, nil
}

/* --------------------- WRITE ----------------------- */

func (d *Dedup) StoreState() error {
	content, err := json.Marshal(d.ops)
	if err != nil {
		return err
	}

	// Replace entire file
	if _, err := d.file.Seek(0, 0); err != nil {
		return err
	}
	if err := d.file.Truncate(0); err != nil {
		return err
	}

	if _, err := d.file.Write(content); err != nil {
		return err
	}

	return d.file.Sync()
}

/* --------------------- READ ----------------------- */

func (d *Dedup) RestoreState() error {
	if _, err := d.file.Seek(0, 0); err != nil {
		return err
	}

	info, err := d.file.Stat()
	if err != nil {
		return err
	}

	// Empty file = empty state
	if info.Size() == 0 {
		d.ops = make(map[string]int)
		d.state = 0
		return nil
	}

	content := make([]byte, info.Size())
	if _, err := d.file.Read(content); err != nil {
		return err
	}

	var ops map[string]int
	if err := json.Unmarshal(content, &ops); err != nil {
		return err
	}

	sum := 0
	for _, v := range ops {
		sum += v
	}

	d.ops = ops
	d.state = sum
	return nil
}

/* --------------------- ADD OPERATION ------------------------ */

func (d *Dedup) AddOperation(opID string, delta int) error {
	if _, ok := d.ops[opID]; ok {
		return fmt.Errorf("operation already applied: %s", opID)
	}

	d.ops[opID] = delta
	if err := d.StoreState(); err != nil {
		delete(d.ops, opID)
		return err
	}

	d.state += delta
	return nil
}

/* -------------------- DRIVER ---------------------- */

func main() {
	d, err := InitDedup()
	// if err != nil {
	// 	fmt.Println("init error:", err)
	// 	return
	// }
	//
	// _ = d.AddOperation("op-1", 10)
	// _ = d.AddOperation("op-2", 11)
	//
	err = d.AddOperation("op-2", 99)
	fmt.Println("state:", d.state)
	fmt.Println("error:", err)
}

