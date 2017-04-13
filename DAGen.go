package main

import (
	"os"
	"path"
	"time"

	"./config"
	"./fs"
	"./model"
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

	startTime := time.Now()
	for i, file := range files {
		if i < 100 {
			println("Loading:", file.Name(), file.ModTime().String())
			var activities model.AccountActivityBatch
			count := activities.LoadAACFile(path.Join(config.IO.InputDIR, file.Name()))
			println("Loaded records:", count)

			// record, _ := json.Marshal(activities.Batch[0])
			// println("First record:", string(record))
			//
			// record, _ = json.Marshal(activities.Batch[activities.Count-1])
			// println("Last record:", string(record))
		}
	}
	println("Elapsed time:", time.Since(startTime).Seconds())
}
