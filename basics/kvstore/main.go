package main

import (
	"fmt"
	"os"
	"encoding/json"
	"time"
)

type Record struct {
	Key string `json:"key"`
	Value string `json:"value"`
	ExpireAt time.Time `json:"expire_at"`
}

const fileName = ".output/kvstore.json"


type KVStore struct  {
	mapping map[string]Record
}

func NewKVStore() *KVStore {
	return &KVStore{
		mapping: make(map[string]Record),
	}
}

func (kv *KVStore) initKVStore() {
	kv.readRecords()
}


func (kv *KVStore) get(key string) (string, error) {
	currEntry, ok := kv.mapping[key]
	now := time.Now()
	if !ok {
		return "", fmt.Errorf("Key not found %v", key)
	}
	if currEntry.ExpireAt.Before(now) {
		delete(kv.mapping, key)
		kv.writeRecords()
		return "", fmt.Errorf("Key not found %v", key)
	}
	return currEntry.Value, nil
}

func (kv *KVStore) set(key, value string, ttl time.Duration) {
	entry := Record { Key: key, Value: value, ExpireAt: time.Now().Add(ttl) }
	kv.mapping[key] = entry
	kv.writeRecords()
}

func (kv *KVStore) delete(key string) error {
	_, ok := kv.mapping[key]
	if !ok {
		return fmt.Errorf("Key not found %v", key)
	}
	delete(kv.mapping, key)
	kv.writeRecords()
	return nil
}

func (kv *KVStore) writeRecords() {
	byteContent, _ := json.Marshal(kv.mapping)
	os.WriteFile(fileName, byteContent, 0644)
}

func (kv *KVStore) readRecords() {
	content, _ := os.ReadFile(fileName)
	var result map[string]Record
	json.Unmarshal(content, &result)
	kv.mapping = result
}


func main() {
	kvstore := NewKVStore()
	kvstore.initKVStore()

	// kvstore.set("Sudhanshu10", "Joshi10", time.Second * 100)

	result, err := kvstore.get("Sudhanshu10")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(result)
}
