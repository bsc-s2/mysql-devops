package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type Connection struct {
	Addr     string // can be ip:port or a unix socket domain
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

type DBInfo struct {
	Conn  *client.Conn
	Shard *DBShard
}

type DBShard struct {
	From   []string
	Table  string
	DBPort string
}

type WriteEvent struct {
	event  *replication.BinlogEvent
	before map[string]string
	after  map[string]string
	dbInfo *DBInfo
}

type OutStatus struct {
	err            error
	goroutineIndex int
	logPos         int32
}

type Config struct {
	// this config used to parse `config.json`, format detail is in the example config file
	// `config.json`

	// WriteThreadCnt specifies how many goroutine used to execute sql statement
	WriteWorkerCnt int

	SourceConn Connection

	DBAddrs map[string]Connection
	Shards  []DBShard

	TableName  string
	TableField []string
	TableShard []string
	TableIndex []string

	// if set GTID, binlog file and pos will be ignored
	GTID string

	BinlogFile string
	BinlogPos  int32

	// TickCnt specifies every `TickCnt` rows synced got 1 status report
	TickCnt int64
}

var (
	mutex *sync.Mutex

	wg sync.WaitGroup

	fileLog  *log.Logger
	shellLog *log.Logger
)

var (
	dbPool          = make(map[string]*client.Conn)
	conf            = Config{}
	logFileName     = "binlog_sync.out"
	confName        = "./config.json"
	channelCapacity = 10240
)

func validRow(srcRow []interface{}) map[string]string {
	// the first column `id` should not put in new rowValue
	rowValue := make([]string, len(srcRow)-1)
	for i, v := range srcRow[1:] {
		if v == nil {
			continue
		}

		var tmp string
		if reflect.TypeOf(v).String() == "[]uint8" {
			// convert []byte to string
			tmp = fmt.Sprintf("%s", v)
		} else {
			tmp = fmt.Sprintf("%v", v)
		}

		rowValue[i] = tmp
	}

	row := make(map[string]string)

	for i, k := range conf.TableField {
		row[k] = rowValue[i]
	}

	return row
}

func newBinlogSyncer(conn *Connection, serverID int32) (*replication.BinlogSyncer, error) {
	port, err := strconv.ParseInt(conn.Port, 10, 16)
	if err != nil {
		return nil, err
	}

	binlogCfg := replication.BinlogSyncerConfig{
		ServerID: uint32(serverID),
		Flavor:   "mysql",
		Host:     conn.Host,
		Port:     uint16(port),
		User:     conn.User,
		Password: conn.Password,
	}

	return replication.NewBinlogSyncer(binlogCfg), nil
}

func newBinlogReaderByPosition(conn *Connection, binlogFile string, binlogPos int32, serverID int32) (*replication.BinlogStreamer, error) {

	syncer, err := newBinlogSyncer(conn, serverID)
	if err != nil {
		return nil, err
	}

	streamer, err := syncer.StartSync(mysql.Position{binlogFile, uint32(binlogPos)})
	if err != nil {
		return nil, err
	}

	return streamer, nil
}

func newBinlogReaderByGTID(conn *Connection, GTID string, serverID int32) (*replication.BinlogStreamer, error) {
	syncer, err := newBinlogSyncer(conn, serverID)
	if err != nil {
		return nil, err
	}

	gtidSet, err := mysql.ParseMysqlGTIDSet(GTID)
	if err != nil {
		return nil, err
	}

	streamer, err := syncer.StartSyncGTID(gtidSet)
	if err != nil {
		return nil, err
	}

	return streamer, nil
}

func writeToDB(chIdx int, inCh chan *WriteEvent, outCh chan *OutStatus) {
	for ev := range inCh {

		if ev.event.Header.EventType == replication.ROTATE_EVENT {
			continue
		}

		mutex.Lock()
		ev.dbInfo = getEventConnection(ev.after)
		mutex.Unlock()

		index := makeTableIndex(ev)

		value := make([]string, len(conf.TableField))
		for i, k := range conf.TableField {
			value[i] = ev.after[k]
		}

		var sql string
		evType := ev.event.Header.EventType

		fileLog.Printf("routin index: %d, before: %v, after: %v, event type: %v\n", chIdx, ev.before, ev.after, evType)

		if evType == replication.UPDATE_ROWS_EVENTv2 || evType == replication.UPDATE_ROWS_EVENTv1 || evType == replication.UPDATE_ROWS_EVENTv0 {
			sql = makeUpdateSql(ev.dbInfo.Shard.Table, conf.TableIndex, index, conf.TableField, value)

		} else if evType == replication.DELETE_ROWS_EVENTv2 || evType == replication.DELETE_ROWS_EVENTv1 || evType == replication.DELETE_ROWS_EVENTv0 {

			sql = makeDeleteSql(ev.dbInfo.Shard.Table, conf.TableIndex, index)
		} else if evType == replication.WRITE_ROWS_EVENTv2 || evType == replication.WRITE_ROWS_EVENTv1 || evType == replication.WRITE_ROWS_EVENTv0 {

			sql = makeInsertSql(ev.dbInfo.Shard.Table, conf.TableField, value)
		} else {
			continue
		}

		fileLog.Printf("routin index: %d, get sql statement: %v", chIdx, sql)

		mutex.Lock()
		_, err := ev.dbInfo.Conn.Execute(sql)
		mutex.Unlock()
		if err != nil {
			fileLog.Printf("Execute error: %v\n", err)
		}

		stat := &OutStatus{
			err:            err,
			goroutineIndex: chIdx,
			logPos:         int32(ev.event.Header.LogPos),
		}

		fileLog.Printf("routin index: %d, event position: %d\n", chIdx, ev.event.Header.LogPos)
		outCh <- stat
	}
}

func collector(inCh chan *OutStatus) {
	var rowCount int64
	var errCount int64
	var errTypes = make(map[string]int)

	var start = time.Now()
	for outStat := range inCh {
		if outStat.err != nil {
			errCount += 1
			errTypes[outStat.err.Error()] += 1
		}
		rowCount += 1

		if rowCount%conf.TickCnt == 0 {

			shellLog.Printf("========= sync stat =========\n")

			shellLog.Printf("has synced: %d rows\n", rowCount)
			shellLog.Printf("has error: %d rows\n", errCount)
			for k, v := range errTypes {
				shellLog.Printf("%s: %d rows\n", k, v)
			}

			shellLog.Printf("sync rate: %.3f rows per second\n", float64(conf.TickCnt)/time.Since(start).Seconds())
			shellLog.Printf("has synced log position: %d\n", outStat.logPos)

			start = time.Now()
		}
	}
}

func getEventConnection(row map[string]string) *DBInfo {
	shardValues := make([]string, len(conf.TableShard))
	for i, k := range conf.TableShard {
		shardValues[i] = row[k]
	}

	shard := findShards(shardValues)
	conn := dbPool[shard.DBPort]
	if conn == nil {
		addr := conf.DBAddrs[shard.DBPort]
		conn, err := client.Connect(addr.Addr, addr.User, addr.Password, addr.DBName)
		if err != nil {
			fileLog.Panicf("get connection failed: %v\n", err)
		}
		dbPool[addr.Port] = conn
	}

	return &DBInfo{
		Conn:  conn,
		Shard: shard,
	}
}

func findShards(tbShards []string) *DBShard {

	//conf.Shards should be descending
	i := sort.Search(len(conf.Shards), func(i int) bool {
		shard := conf.Shards[i].From
		rst, err := compareStringSlice(shard, tbShards)
		if err != nil {
			fileLog.Panicf("conf is not valid, shard length not equal: %v\n", err)
		}

		if rst <= 0 {
			return true
		}
		return false
	})

	if i >= 0 && i < len(conf.Shards) {
		return &conf.Shards[i]
	}

	fileLog.Panicf("can not find shard: index out of bound")
	return nil
}

func main() {

	// set log
	shellLog = log.New(os.Stdout, "", 0)

	logFile, err := os.Create(logFileName)
	if err != nil {
		shellLog.Panicf("create log file failed: %v\n", err)
	}
	defer logFile.Close()
	fileLog = log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile)

	// read config
	jsonParser := NewJsonStruct()

	err = jsonParser.Load(confName, &conf)
	if err != nil {
		shellLog.Panicf("read config file failed: %v\n", err)
	}

	var writeChs = make([]chan *WriteEvent, conf.WriteWorkerCnt)
	var countCh = make(chan *OutStatus, conf.WriteWorkerCnt*channelCapacity)

	for i := 0; i < conf.WriteWorkerCnt; i++ {
		writeCh := make(chan *WriteEvent, channelCapacity)
		writeChs[i] = writeCh

		wg.Add(1)
		go writeToDB(i, writeCh, countCh)
	}

	go collector(countCh)

	var binlogReader *replication.BinlogStreamer
	if conf.GTID != "" {
		binlogReader, err = newBinlogReaderByGTID(&conf.SourceConn, conf.GTID, 9999)
	} else {
		binlogReader, err = newBinlogReaderByPosition(&conf.SourceConn, conf.BinlogFile, conf.BinlogPos, 9999)
	}

	if err != nil {
		shellLog.Panicf("make binlog reader failed: %v\n", err)
	}

	for {
		ev, err := binlogReader.GetEvent(context.Background())
		if err != nil {
			shellLog.Panicf("get event failed: %v\n", err)
		}

		if ev.Header.EventType == replication.ROTATE_EVENT {
			writeEV := &WriteEvent{
				event: ev,
			}
			writeChs[0] <- writeEV
			continue
		}

		writeEV := makeWriteEvent(ev)
		if writeEV == nil {
			continue
		}

		indexValues := make([]string, len(conf.TableIndex))
		for i, k := range conf.TableIndex {
			if writeEV.before != nil {
				indexValues[i] = writeEV.before[k]
			} else {
				indexValues[i] = writeEV.after[k]
			}
		}

		rowHash, err := hashStringSliceToInt32(indexValues)
		if err != nil {
			shellLog.Panicf("calculate hash failed: %v", err)
		}

		chIdx := rowHash % int64(conf.WriteWorkerCnt)
		writeChs[chIdx] <- writeEV
	}

	wg.Wait()
}

func makeWriteEvent(ev *replication.BinlogEvent) *WriteEvent {
	var before map[string]string
	var after map[string]string

	rowEv, ok := ev.Event.(*replication.RowsEvent)
	if !ok {
		fileLog.Printf("event is not a rows event, got: %v\n", ev.Header.EventType)
		return nil
	}

	table := string(rowEv.Table.Table)
	if table != conf.TableName {
		fileLog.Printf("rows event is not the required table, get %v\n", table)
		return nil
	}

	if len(rowEv.Rows) == 2 {
		before = validRow(rowEv.Rows[0])
		after = validRow(rowEv.Rows[1])
	} else {
		before = nil
		after = validRow(rowEv.Rows[0])
	}

	return &WriteEvent{
		event:  ev,
		before: before,
		after:  after,
	}
}

func makeTableIndex(ev *WriteEvent) []string {
	index := make([]string, len(conf.TableIndex))
	for i, k := range conf.TableIndex {
		if ev.before != nil {
			index[i] = ev.before[k]
		} else {
			index[i] = ev.after[k]
		}
	}

	return index
}
