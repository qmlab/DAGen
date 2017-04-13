package main

import (
	"os"

	"./config"
	"./fs"
)

func main() {
	println("Service starts")
	configFile := "./config/service.json"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}
	println(configFile)

	var config config.ServiceConfig
	config.LoadConfig(configFile)

	files := fs.LoadFilesByTime(config.IO.InputDIR)

	for i, file := range files {
		if i < 10 {
			println(file.Name(), file.ModTime().String())
		}
	}
}
