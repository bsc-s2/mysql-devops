package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/replication"
)

type Connection struct {
	Addr     string `yaml:"Addr"` // can be ip:port or a unix socket domain
	Host     string `yaml:"Host"`
	Port     string `yaml:"Port"`
	User     string `yaml:"User"`
	Password string `yaml:"Password"`
	DBName   string `yaml:"DBName"`
}

type DBInfo struct {
	Conn  *client.Conn
	Shard *DBShard
}

type DBShard struct {
	From   []interface{} `yaml:"From"`
	Table  string        `yaml:"Table"`
	DBPort string        `yaml:"DBPort"`
}

type WriteEvent struct {
	event    *replication.BinlogEvent
	before   map[string]interface{}
	after    map[string]interface{}
	dbInfo   *DBInfo
	nextGTID string
}

type BinlogSyncer struct {
	Config

	DBPool   map[string]*client.Conn
	WriteChs []chan *WriteEvent
	CountCh  chan *Status

	mutex    *sync.Mutex
	wg       *sync.WaitGroup
	shellLog *log.Logger
	fileLog  *log.Logger
}

type BinlogPosition struct {
	BinlogFile string `yaml:"BinlogFile"`
	BinlogPos  int64  `yaml:"BinlogPos"`
	NextGTID   string `yaml:"GTID"`
}

type OutMessage struct {
	Synced   int64          `yaml:"Synced"`
	Faild    int64          `yaml:"Faild"`
	Rate     float64        `yaml:"Rate(rows/s)"`
	Position BinlogPosition `yaml:"Position"`
	Error    map[string]int `yaml:"Error"`
}

type Status struct {
	err            error
	goroutineIndex int

	position BinlogPosition
}

func NewBinlogSyncer(conf Config) *BinlogSyncer {
	fileLog := log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile)
	fileLog.SetPrefix("[" + conf.SourceConn.Addr + "] ")

	bs := &BinlogSyncer{
		Config: conf,

		DBPool:   make(map[string]*client.Conn),
		WriteChs: make([]chan *WriteEvent, conf.WorkerCnt),
		CountCh:  make(chan *Status, channelCapacity*conf.WorkerCnt),

		mutex:    &sync.Mutex{},
		wg:       &sync.WaitGroup{},
		shellLog: shellLog,
		fileLog:  fileLog,
	}

	return bs
}

func (bs *BinlogSyncer) Sync() {
	defer mainWG.Done()

	var binlogReader *replication.BinlogStreamer
	var err error

	if bs.GTIDSet != "" {
		binlogReader, err = newBinlogReaderByGTID(&bs.SourceConn, bs.GTIDSet, 9999)
	} else {
		binlogReader, err = newBinlogReaderByPosition(&bs.SourceConn, bs.BinlogFile, bs.BinlogPos, 9999)
	}

	if err != nil {
		bs.shellLog.Printf("[%s] make binlog reader failed: %v\n", bs.SourceConn.Addr, err)
		return
	}

	for i := 0; i < bs.WorkerCnt; i++ {
		writeCh := make(chan *WriteEvent, channelCapacity)
		bs.WriteChs[i] = writeCh
		bs.wg.Add(1)
		go bs.writeToDB(i, writeCh)
	}

	go bs.readBinlog(binlogReader)

	go func() {
		bs.wg.Wait()
		close(bs.CountCh)
	}()

	bs.collector()
}

func (bs *BinlogSyncer) formatRow(srcRow []interface{}) map[string]interface{} {
	// the first column `id` should not put in new rowValue
	rowValue := make([]interface{}, len(srcRow)-1)
	for i, v := range srcRow[1:] {
		if v == nil {
			continue
		}

		var tmp interface{}
		if reflect.TypeOf(v).String() == "[]uint8" {
			// convert []byte to string
			tmp = fmt.Sprintf("%s", v)
		} else {
			tmp = v
		}

		rowValue[i] = tmp
	}

	row := make(map[string]interface{})

	for i, k := range bs.TableField {
		row[k] = rowValue[i]
	}

	return row
}

func (bs *BinlogSyncer) writeToDB(chIdx int, inCh chan *WriteEvent) {
	defer bs.wg.Done()

	for ev := range inCh {
		if ev.event.Header.EventType == replication.ROTATE_EVENT {
			rotateEv, ok := ev.event.Event.(*replication.RotateEvent)
			if !ok {
				bs.fileLog.Printf("event is RotateEvent, but cannot convert to a RotateEvent")
			}
			stat := &Status{
				goroutineIndex: chIdx,
				position: BinlogPosition{
					BinlogPos:  int64(rotateEv.Position),
					BinlogFile: string(rotateEv.NextLogName),
				},
			}
			bs.CountCh <- stat
			continue
		}

		var err error
		ev.dbInfo, err = bs.getEventConnection(ev.after)
		if err != nil {
			bs.shellLog.Printf("[%s] get connection failed: %v\n", bs.SourceConn.Addr, err)
			return
		}

		index := bs.makeTableIndex(ev)

		value := make([]string, len(bs.TableField))
		for i, k := range bs.TableField {
			value[i] = fmt.Sprintf("%v", ev.after[k])
		}

		var sql string
		evType := ev.event.Header.EventType

		bs.fileLog.Printf("routin index: %d, before: %v, after: %v, event type: %v\n", chIdx, ev.before, ev.after, evType)

		if evType == replication.UPDATE_ROWS_EVENTv2 || evType == replication.UPDATE_ROWS_EVENTv1 || evType == replication.UPDATE_ROWS_EVENTv0 {
			sql = makeUpdateSql(ev.dbInfo.Shard.Table, bs.TableIndex, bs.TableField, index, value)

		} else if evType == replication.DELETE_ROWS_EVENTv2 || evType == replication.DELETE_ROWS_EVENTv1 || evType == replication.DELETE_ROWS_EVENTv0 {

			sql = makeDeleteSql(ev.dbInfo.Shard.Table, bs.TableIndex, index)
		} else if evType == replication.WRITE_ROWS_EVENTv2 || evType == replication.WRITE_ROWS_EVENTv1 || evType == replication.WRITE_ROWS_EVENTv0 {

			sql = makeInsertSql(ev.dbInfo.Shard.Table, bs.TableField, value)
		} else {
			continue
		}

		bs.fileLog.Printf("routin index: %d, get sql statement: %v", chIdx, sql)

		bs.mutex.Lock()
		_, err = ev.dbInfo.Conn.Execute(sql)
		bs.mutex.Unlock()
		if err != nil {
			bs.fileLog.Printf("Execute error: %v\n", err)
		}

		stat := &Status{
			err:            err,
			goroutineIndex: chIdx,
			position: BinlogPosition{
				BinlogPos:  int64(ev.event.Header.LogPos),
				BinlogFile: bs.BinlogFile,
				NextGTID:   ev.nextGTID,
			},
		}

		bs.fileLog.Printf("routin index: %d, event position: %d\n", chIdx, ev.event.Header.LogPos)
		bs.CountCh <- stat
	}
}

func (bs *BinlogSyncer) readBinlog(binlogReader *replication.BinlogStreamer) {
	var gtidNext string
	for {
		ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
		ev, err := binlogReader.GetEvent(ctx)
		if err != nil {
			bs.fileLog.Printf("get event failed: %v\n", err)
			break
		}

		if ev.Header.EventType == replication.ROTATE_EVENT {
			writeEV := &WriteEvent{
				event: ev,
			}
			// RotateEvent has no row to hash so put it to channal 0
			bs.WriteChs[0] <- writeEV
			continue
		}

		if ev.Header.EventType == replication.GTID_EVENT {
			gtidEv, ok := ev.Event.(*replication.GTIDEvent)
			if !ok {
				bs.fileLog.Printf("event is GTIDEvent, but cannot convert to a GTIDEvent")
				continue
			}

			u, _ := uuid.FromBytes(gtidEv.SID)
			gtidNext = fmt.Sprintf("%s:%d", u.String(), gtidEv.GNO)

			bs.fileLog.Printf("next gtid: %s\n", gtidNext)
			continue
		}

		writeEV := bs.makeWriteEvent(ev)
		if writeEV == nil {
			// not a RowsEvent
			continue
		}
		writeEV.nextGTID = gtidNext

		indexValues := bs.makeTableIndex(writeEV)

		rowHash, err := hashStringSliceToInt32(indexValues)
		if err != nil {
			bs.shellLog.Printf("[%s] calculate hash failed: %v", bs.SourceConn.Addr, err)
		}

		chIdx := rowHash % int64(bs.WorkerCnt)
		bs.WriteChs[chIdx] <- writeEV
	}

	for _, ch := range bs.WriteChs {
		close(ch)
	}
}

func (bs *BinlogSyncer) collector() {
	var rowCnt int64
	var errCnt int64
	var errTypes = make(map[string]int)

	var start = time.Now()
	for outStat := range bs.CountCh {
		if outStat.err != nil {
			errCnt += 1
			errTypes[outStat.err.Error()] += 1
		}
		rowCnt += 1

		if rowCnt%bs.TickCnt == 0 {

			rate := float64(bs.TickCnt) / time.Since(start).Seconds()

			om := &OutMessage{
				Synced:   rowCnt,
				Faild:    errCnt,
				Rate:     rate,
				Position: outStat.position,
				Error:    errTypes,
			}

			outMessage, err := dumpYAML(om)
			if err != nil {
				bs.fileLog.Printf("[%s] dump out message to YAML failed: %v\n", bs.SourceConn.Addr, outMessage)
				return
			}

			outMessage = " ====== [" + bs.SourceConn.Addr + "] status ======\n" + outMessage
			bs.shellLog.Printf(outMessage)

			start = time.Now()
		}
	}
}

func (bs *BinlogSyncer) getEventConnection(row map[string]interface{}) (*DBInfo, error) {
	shardValues := make([]interface{}, len(bs.TableShard))
	for i, k := range bs.TableShard {
		shardValues[i] = row[k]
	}

	shard := bs.findShards(shardValues)

	bs.mutex.Lock()
	conn := bs.DBPool[shard.DBPort]
	if conn == nil {
		addr := bs.DBConfig[shard.DBPort]

		var err error
		conn, err = client.Connect(addr.Addr, addr.User, addr.Password, addr.DBName)
		if err != nil {
			bs.shellLog.Printf("[%s] get connection failed: %v\n", bs.SourceConn, err)
			return nil, err
		}
		bs.DBPool[addr.Port] = conn
	}
	bs.mutex.Unlock()

	dbInfo := &DBInfo{
		Conn:  conn,
		Shard: shard,
	}
	return dbInfo, nil
}

func (bs *BinlogSyncer) findShards(tbShards []interface{}) *DBShard {

	//conf.Shards should be descending
	i := sort.Search(len(bs.Shards), func(i int) bool {
		shard := bs.Shards[i].From
		rst, err := compareSlice(shard, tbShards)
		if err != nil {
			bs.shellLog.Printf("[%s] compare table shards failed: %v\n", bs.SourceConn.Addr, err)
		}

		if rst <= 0 {
			return true
		}
		return false
	})

	if i >= 0 && i < len(bs.Shards) {
		return &bs.Shards[i]
	}

	bs.shellLog.Printf("[%s] can not find shard: index out of bound", bs.SourceConn.Addr)
	return nil
}

func (bs *BinlogSyncer) makeWriteEvent(ev *replication.BinlogEvent) *WriteEvent {
	var before map[string]interface{}
	var after map[string]interface{}

	rowEv, ok := ev.Event.(*replication.RowsEvent)
	if !ok {
		bs.fileLog.Printf("event is not a rows event, got: %v\n", ev.Header.EventType)
		return nil
	}

	table := string(rowEv.Table.Table)
	if table != bs.TableName {
		bs.fileLog.Printf("rows event is not the required table, get %v\n", table)
		return nil
	}

	if len(rowEv.Rows) == 2 {
		before = bs.formatRow(rowEv.Rows[0])
		after = bs.formatRow(rowEv.Rows[1])
	} else {
		before = nil
		after = bs.formatRow(rowEv.Rows[0])
	}

	return &WriteEvent{
		event:  ev,
		before: before,
		after:  after,
	}
}

func (bs *BinlogSyncer) makeTableIndex(ev *WriteEvent) []string {
	index := make([]string, len(bs.TableIndex))
	for i, k := range bs.TableIndex {
		if ev.before != nil {
			index[i] = fmt.Sprintf("%v", ev.before[k])
		} else {
			index[i] = fmt.Sprintf("%v", ev.after[k])
		}
	}

	return index
}