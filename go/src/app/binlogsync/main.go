package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	logFileName     = "binlog_sync.out"
	confName        = "./config.json"
	channelCapacity = 10240

	logFile  *os.File
	shellLog *log.Logger

	mainWG *sync.WaitGroup
)

type Config struct {

	// WorkerCnt specifies how many goroutine used to execute sql statement
	WorkerCnt int `yaml:"WriteWorkerCnt"`

	SourceConn Connection `yaml:"SourceConn"`

	DBConfig map[string]Connection `yaml:"DBConfig"`
	Shards   []DBShard             `yaml:"Shards"`

	TableName  string   `yaml:"TableName"`
	TableField []string `yaml:"TableField"`
	TableShard []string `yaml:"TableShard"`
	TableIndex []string `yaml:"TableIndex"`

	// if specifies GTIDSet, binlog file and pos will be ignored
	GTIDSet string `yaml:"GTIDSet"`

	BinlogFile string `yaml:"BinlogFile"`
	BinlogPos  int32  `yaml:"BinlogPos"`

	// TickCnt specifies every `TickCnt` rows synced got 1 status report
	TickCnt int64 `yaml:"TickCnt"`
}

type ConfigList struct {
	Confs []Config `yaml:"Confs"`
}

func main() {

	// get arguments
	getInput()

	// set log
	var err error
	shellLog = log.New(os.Stdout, "", 0)
	logFile, err = os.Create(logFileName)
	if err != nil {
		shellLog.Panicf("create log file failed: %v\n", err)
	}
	defer logFile.Close()

	// read config
	confList := &ConfigList{}
	err = unmarshalYAML(confName, &confList)
	if err != nil {
		shellLog.Panicf("read config file failed: %v\n", err)
	}

	mainWG = &sync.WaitGroup{}
	for _, conf := range confList.Confs {
		syncer := NewBinlogSyncer(conf)
		mainWG.Add(1)
		syncer.Sync()
	}
	mainWG.Wait()
}

func getInput() {
	flag.StringVar(&logFileName, "log", logFileName, "file name to output error log")
	flag.StringVar(&confName, "config", confName, "configration file path")

	flag.Parse()
	fmt.Printf("log: %v, conf: %v\n", logFileName, confName)
}
