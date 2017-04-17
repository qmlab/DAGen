package main

import (
	"os"
	"path"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"./common"
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

	// Load config
	var config config.ServiceConfig
	config.LoadConfig(configFile)

	// Load files from source
	dir := config.IO.InputDIR
	aac, _ := removeUnpairedFiles(fs.LoadFilesByTime(dir))

	// Init DB connection
	session := initDB(config)
	defer session.Close()
	cAcc := session.DB("db-data").C("account")
	cAccDA := session.DB("db-da").C("account")

	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < config.Routines; i++ {
		wg.Add(1)
		batch := model.NewAccountActivityBatch()
		var op model.AccountActivityOperation
		go process(aac, dir, i, config.Routines, batch, op, cAcc, cAccDA, &wg)
	}

	// Wait till all goroutines are done
	wg.Wait()
	println("Elapsed time:", time.Since(startTime).Seconds())
}

func removeUnpairedFiles(files []os.FileInfo) (aac []os.FileInfo, sac []os.FileInfo) {
	counts := make(map[string]int16)
	for _, file := range files {
		name := file.Name()
		key := name[:len(name)-4]
		if _, ok := counts[key]; ok {
			counts[key]++
		} else {
			counts[key] = 1
		}
	}

	for _, file := range files {
		name := file.Name()
		key := name[:len(name)-4]

		if v, ok := counts[key]; ok {
			if v == 2 {
				ext := name[len(name)-4:]
				if ext == ".aac" {
					aac = append(aac, file)
				}
				if ext == ".sac" {
					sac = append(sac, file)
				}
			}
		}
	}

	return
}

func initDB(config config.ServiceConfig) *mgo.Session {
	session, err := mgo.Dial(config.Database.ConnStr)
	if err != nil {
		panic(err)
	}
	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	return session
}

func process(files []os.FileInfo, dir string, shard int, routines int, batch model.IActivityBatch, op model.IActivityOperation, cData *mgo.Collection, cDA *mgo.Collection, wg *sync.WaitGroup) {
	for i := 0; i < len(files); i++ {
		file := files[i]
		hash := int(util.Hash(file.Name()))
		if hash%routines == shard {
			batch.Clear()
			println("Shard", shard, "loading", file.Name(), file.ModTime().String())
			count := batch.LoadDataFile(path.Join(dir, file.Name()))
			println("Shard", shard, "loaded records:", count)

			// If there is any record
			if count > 0 {
				batchname, provider, version := batch.GetKeys()

				// Load existing records
				query := cData.Find(bson.M{"batchname": batchname, "adviceprovider": provider}).Sort("-versionnumber")
				if n, e := query.Count(); n > 0 && e == nil {
					lastVer := op.GetLastVersion(query)
					if version > lastVer {
						// Add the current version to data db
						batch.InsertToStore(cData)

						// Load last version
						batch.GetAndCompareLastBatch(batchname, provider, version, lastVer, cData, cDA)

						// Put remaining new records to DA
						batch.InsertToStore(cDA)
					}
				} else {
					// If new file, write to both data and DA stores
					batch.InsertToStore(cData)
					batch.InsertToStore(cDA)
				}
			}
		}
	}

	defer wg.Done()
}
