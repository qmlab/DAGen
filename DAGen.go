package main

import (
	"os"
	"path"
	"sync"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"./cache"
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
	epaDir := config.IO.EPADIR
	txDir := config.IO.TxDIR

	// Init DB connection
	session := initDB(config)
	defer session.Close()
	cAcc := session.DB("db-data").C("account")
	cAccDA := session.DB("db-da").C("account")
	cSub := session.DB("db-data").C("submission")
	cSubDA := session.DB("db-da").C("submission")
	cTx := session.DB("db-data").C("transaction")

	// Load cache from store
	versions := getVersions(cAcc, config.Routines)
	mutexes := make(map[uint32]*sync.Mutex)
	for k := range versions {
		mutexes[k] = &sync.Mutex{}
	}

	startTime := time.Now()
	cachedFiles := fs.LoadFilesByName(epaDir)
	aac, sac := removeUnpairedFiles(cachedFiles)
	cache := cache.New(util.LRUCacheSize)
	for {
		txErr := model.LoadTxFile(txDir, cTx)
		var wg sync.WaitGroup
		for i := 0; i < config.Routines; i++ {
			wg.Add(1)
			subBatch := model.NewSubmissionActivityBatch()
			subBatch.Cache = &cache
			var sacOp model.SubmissionActivityOperation
			go process(sac, epaDir, i, config.Routines, subBatch, sacOp, versions, cSub, cSubDA, cTx, &wg, mutexes)
		}

		for i := 0; i < config.Routines; i++ {
			wg.Add(1)
			accBatch := model.NewAccountActivityBatch()
			var aacOp model.AccountActivityOperation
			go process(aac, epaDir, i, config.Routines, accBatch, aacOp, versions, cAcc, cAccDA, cTx, &wg, mutexes)
		}

		// Wait till all goroutines are done
		wg.Wait()

		fs.DeleteFiles(epaDir, aac)
		fs.DeleteFiles(epaDir, sac)

		if len(cachedFiles) > 0 || txErr == nil {
			println("Elapsed time:", time.Since(startTime).Seconds())
		}

		// Next round
		time.Sleep(5 * time.Second)
		cachedFiles = fs.LoadFilesByName(epaDir)
		aac, sac = removeUnpairedFiles(cachedFiles)
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
		hash := getKeyHashCode(vtable.Keys["batchname"], vtable.Keys["provider"])
		versions[hash] = vtable.Version
	}
	return
}

func getKeyHashCode(filename string, provider string) uint32 {
	return util.Hash(filename + "|" + provider)
}

func process(files []os.FileInfo, dir string, shard int, routines int, batch model.IActivityBatch, op model.IActivityOperation, versions map[uint32]uint32, cData *mgo.Collection, cDA *mgo.Collection, cTx *mgo.Collection, wg *sync.WaitGroup, mutexes map[uint32]*sync.Mutex) {
	for i := 0; i < len(files); i++ {
		file := files[i]
		hash := int(util.Hash(file.Name()))
		if hash%routines == shard {
			batch.Clear()
			println("Shard", shard, "is loading", file.Name(), file.ModTime().String())
			count := batch.LoadDataFile(path.Join(dir, file.Name()))
			println("Shard", shard, "has loaded from file:[", file.Name(), "] count:", count)

			// If there is any record
			if count > 0 {
				batchname, provider, version := batch.GetKeys()

				// Load existing records
				h := getKeyHashCode(batchname, provider)
				if _, ok := mutexes[h]; !ok {
					mutexes[h] = &sync.Mutex{}
				}

				// Lock on the file+provider level
				mutexes[h].Lock()
				lastVer, ok := versions[h]
				if ok && version > lastVer {
					// Load Additional properties from tx
					batch.LoadAdditionalProperties(cTx)

					// Add the current version to data db
					batch.InsertToStore(cData)

					// Load last version, compare and add new DA activities, and remove updates/deletes from remaining batch
					batch.GetAndCompareLastBatch(batchname, provider, version, lastVer, cData, cDA)

					// Put remaining new records to DA
					batch.InsertToStore(cDA)
					println("Shard", shard, "has compared and updated from file:[", file.Name(), "] count:", count)
				} else if !ok {
					// Load Additional properties from tx
					batch.LoadAdditionalProperties(cTx)

					// If new file, write to both data and DA stores
					batch.InsertToStore(cData)
					batch.InsertToStore(cDA)
					println("Shard", shard, "has inserted new from file:[", file.Name(), "] count:", count)
				}

				// Update the cached max version table for the shard
				versions[h] = version
				mutexes[h].Unlock()
			}
		}
	}

	defer wg.Done()
}
