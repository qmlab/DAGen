package main

import (
	"log"
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

	// Init DB connection
	session := initDB(config)
	defer session.Close()
	cAcc := session.DB("db-data").C("account")
	cAccDA := session.DB("db-da").C("account")
	cSub := session.DB("db-data").C("submission")
	cSubDA := session.DB("db-da").C("submission")
	versions := getVersions(cAcc, config.Routines)

	startTime := time.Now()

	cachedFiles := fs.LoadFilesByTime(dir)
	aac, sac := removeUnpairedFiles(cachedFiles)
	for {
		var wg sync.WaitGroup
		for i := 0; i < config.Routines; i++ {
			wg.Add(1)
			accBatch := model.NewAccountActivityBatch()
			var aacOp model.AccountActivityOperation
			go process(aac, dir, i, config.Routines, accBatch, aacOp, versions, cAcc, cAccDA, &wg)
		}

		for i := 0; i < config.Routines; i++ {
			wg.Add(1)
			subBatch := model.NewSubmissionActivityBatch()
			var sacOp model.SubmissionActivityOperation
			go process(sac, dir, i, config.Routines, subBatch, sacOp, versions, cSub, cSubDA, &wg)
		}

		// Wait till all goroutines are done
		wg.Wait()

		deleteFiles(dir, aac)
		deleteFiles(dir, sac)

		if len(cachedFiles) > 0 {
			println("Elapsed time:", time.Since(startTime).Seconds())
		}

		// Next round
		time.Sleep(5 * time.Second)
		cachedFiles = fs.LoadFilesByTime(dir)
		aac, sac = removeUnpairedFiles(cachedFiles)
	}
}

func deleteFiles(dir string, files []os.FileInfo) {
	for _, f := range files {
		var err = os.Remove(path.Join(dir, f.Name()))
		if err != nil {
			println("Failed to delete", f.Name())
			log.Fatal(err)
		} else {
			println("Deleted file", f.Name())
		}
	}
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
			ext := name[len(name)-4:]
			// It is legal to have aac without sac, but need to revise later. For now, it requires manual intervention
			if v == 2 /*|| ext == ".aac" && file.Size() < 10000*/ {
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

// VersionTable - max version mapping table
type VersionTable struct {
	Keys    map[string]string `bson:"_id"`
	Version uint32
}

func getVersions(col *mgo.Collection, routines int) (versions map[uint32]uint32) {
	versions = make(map[uint32]uint32)
	stageGroup := bson.M{"$group": bson.M{"_id": bson.M{"batchname": "$batchname", "provider": "$adviceprovider"}, "version": bson.M{"$max": "$versionnumber"}}}
	pipe := col.Pipe([]bson.M{stageGroup})
	var vtables []VersionTable
	pipe.All(&vtables)
	for _, vtable := range vtables {
		hash := getEPAKeyHashCode(vtable.Keys["batchname"], vtable.Keys["provider"])
		versions[hash] = vtable.Version
	}
	return
}

func getEPAKeyHashCode(filename string, provider string) uint32 {
	return util.Hash(filename + "|" + provider)
}

func process(files []os.FileInfo, dir string, shard int, routines int, batch model.IActivityBatch, op model.IActivityOperation, versions map[uint32]uint32, cData *mgo.Collection, cDA *mgo.Collection, wg *sync.WaitGroup) {
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
				h := getEPAKeyHashCode(batchname, provider)
				lastVer, ok := versions[h]
				if ok && version > lastVer {
					// Add the current version to data db
					batch.InsertToStore(cData)

					// Load last version
					batch.GetAndCompareLastBatch(batchname, provider, version, lastVer, cData, cDA)

					// Put remaining new records to DA
					batch.InsertToStore(cDA)
				} else if !ok {
					// If new file, write to both data and DA stores
					batch.InsertToStore(cData)
					batch.InsertToStore(cDA)
				}

				// Update the cached max version table for the shard
				versions[h] = version
			}
		}
	}

	defer wg.Done()
}
