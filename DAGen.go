package main

import (
	"log"
	"os"
	"path"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

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

	// Init DB connection
	session := initDB(config)
	defer session.Close()
	cAcc := session.DB("db-data").C("account")
	cAccDA := session.DB("db-da").C("account")

	// Load files from source
	files := fs.LoadFilesByTime(config.IO.InputDIR)

	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < config.Routines; i++ {
		wg.Add(1)
		go processAAC(files, i, config.Routines, cAcc, cAccDA, config, &wg)
	}

	// Wait till all goroutines are done
	wg.Wait()
	println("Elapsed time:", time.Since(startTime).Seconds())
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

func processAAC(files []os.FileInfo, shard int, routines int, cData *mgo.Collection, cDA *mgo.Collection, config config.ServiceConfig, wg *sync.WaitGroup) {
	for i := shard; i < len(files); i += routines {
		file := files[i]
		println("Shard", shard, "loading", file.Name(), file.ModTime().String())
		account := model.NewAccountActivityBatch()
		count := account.LoadDataFile(path.Join(config.IO.InputDIR, file.Name()))
		println("Loaded records:", count)

		// If there is any record
		if count > 0 {
			var filename string
			var version uint32
			for _, acc := range account.Batch {
				filename = acc.AdviceFileName
				version = acc.VersionNumber
				break
			}

			// Load existing records
			// query := cData.Find(bson.M{"advicefilename": filename}).Sort("-versionnumber")
			query := cData.Find(bson.M{"advicefilename": filename}).Sort("-versionnumber")
			if n, e := query.Count(); n > 0 && e == nil {
				var last model.AccountActivity
				err := query.One(&last)
				if err != nil {
					log.Fatal(err)
				}

				lastVer := last.VersionNumber
				if version > lastVer {
					// Add the current version to data db
					for _, v := range account.Batch {
						err = cData.Insert(&v)
						if err != nil {
							log.Fatal(err)
						}
					}

					// Load last version
					var lastRecords []model.AccountActivity
					now := time.Now().UTC()
					err = cData.Find(bson.M{"advicefilename": filename, "versionnumber": lastVer}).All(&lastRecords)
					if err != nil {
						log.Fatal(err)
					}

					for _, o := range lastRecords {
						hash := o.GetHashCode()
						// If record with same key exists
						if v, ok := account.Batch[hash]; ok {
							diff := v.Amount - o.Amount
							if diff != 0 {
								v.Amount = diff
								err = cDA.Insert(&v)
								if err != nil {
									log.Fatal(err)
								}
							}
							delete(account.Batch, hash)
						} else {
							// If record has been removed
							o.Amount = -o.Amount
							o.LastModifiedTime = now
							err = cDA.Insert(&o)
							if err != nil {
								log.Fatal(err)
							}
						}
					}

					// Put remaining new records to DA
					for _, v := range account.Batch {
						err = cDA.Insert(&v)
						if err != nil {
							log.Fatal(err)
						}
					}
				}
			} else {
				// If new file, write to both data and DA stores
				for _, v := range account.Batch {
					err := cData.Insert(&v)
					if err != nil {
						log.Fatal(err)
					}
					err = cDA.Insert(&v)
					if err != nil {
						log.Fatal(err)
					}
				}
			}

		}
	}

	defer wg.Done()
}
