package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// ServiceConfig - service configuration model
type ServiceConfig struct {
	IO       IOType       `json:"IO"`
	Database DatabaseType `json:"Database"`
	Routines int          `json:"Routines"`
}

// IOType - IO config
type IOType struct {
	InputDIR  string `json:"InputDIR"`
	OutputDIR string `json:"OutputDIR"`
}

// DatabaseType - DB config
type DatabaseType struct {
	ConnStr string `json:"ConnectionString"`
}

// LoadConfig - loads configurations
func (config *ServiceConfig) LoadConfig(file string) {
	content, e := ioutil.ReadFile(file)
	if e != nil {
		println("File error:" + e.Error())
		os.Exit(3)
	}
	json.Unmarshal(content, config)
}
