package main


import (
	"os"
	"fmt"
	"gopkg.in/yaml.v3"
)

type Config struct {
	TopSeed int `yaml:"topspeed"`
	Name string `yaml:"name"`
	Cool bool `yaml:"cool"`
	Passengers []string `yaml:"passengers"`
}


const fileName = ".output/config.yml"

func openConfig(fileName string) (Config, error) {
	defaultConfig := Config {
		TopSeed: 12,
		Name: "Sudhanshu",
		Cool: true,
		Passengers: []string{},
	}

	content, err := os.ReadFile(fileName)
	if err != nil {
		return defaultConfig, fmt.Errorf("file does not exists %v", err)
	}

	var config Config
	err = yaml.Unmarshal(content, &config)
	if err != nil {
		return defaultConfig, fmt.Errorf("config is not correct %v", err)
	}

	return config, nil
}


func main() {
	content, err := openConfig(fileName)
	testName := os.Getenv("TEST_NAME")
	if testName != "" {
		content.Name = testName
	}
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(content)
}
