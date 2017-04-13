package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type ServiceConfig struct {
	IO       IOType       `json:"IO"`
	Database DatabaseType `json:"Database"`
}

type IOType struct {
	InputDIR  string `json:"InputDIR"`
	OutputDIR string `json:"OutputDIR"`
}

type DatabaseType struct {
	ConnStr string `json:"ConnectionString"`
}

func (config *ServiceConfig) LoadConfig(configFile string) {
	content, e := ioutil.ReadFile(configFile)
	if e != nil {
		println("File error:" + e.Error())
		os.Exit(3)
	}
	json.Unmarshal(content, config)
	// bytes, _ := json.Marshal(config)
	// println(string(bytes))
	// println(config.IO.InputDIR)
}
