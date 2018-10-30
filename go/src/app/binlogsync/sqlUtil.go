package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

func makeUpdateSql(table string, idxField, tbField, idxValue, tbValue []string) string {
	var setClause string
	var whereClause string
	var limitClause string
	var tableClause string

	tableClause = quote(table)
	setClause = makeSqlCondition(tbField, tbValue, "=", ", ")
	whereClause = makeWhereClause(idxField, idxValue)
	limitClause = "LIMIT 1"

	return fmt.Sprintf("UPDATE %s SET %s%s%s;", tableClause, setClause, whereClause, limitClause)
}

func makeInsertSql(table string, tbField, tbValue []string) string {
	var fldClause string
	var valClause string
	var tableClause string

	tableClause = quote(table)

	fld := make([]string, len(tbField))
	val := make([]string, len(tbValue))

	for i := 0; i < len(tbField); i++ {
		fld[i] = quote(tbField[i])
		val[i] = "\"" + mysql.Escape(tbValue[i]) + "\""
	}

	fldClause = "(" + strings.Join(fld, ", ") + ")"
	valClause = "(" + strings.Join(val, ", ") + ")"

	return fmt.Sprintf("INSERT INTO %s %s VALUES %s;", tableClause, fldClause, valClause)
}

func makeDeleteSql(table string, idxField, idxValue []string) string {
	var whereClause string
	var limitClause string
	var tableClause string

	tableClause = quote(table)

	whereClause = makeWhereClause(idxField, idxValue)
	limitClause = "LIMIT 1"

	return fmt.Sprintf("DELETE FROM %s %s %s;", tableClause, whereClause, limitClause)
}

func makeWhereClause(fields, values []string) string {
	return fmt.Sprintf("WHERE %s", makeSqlCondition(fields, values, "=", " AND "))
}

func makeSqlCondition(fields, values []string, operator, formatter string) string {
	conds := make([]string, len(fields))

	for i, k := range fields {
		conds[i] = fmt.Sprintf("%s%s%s", quote(k), operator, "\""+mysql.Escape(values[i])+"\"")
	}

	return strings.Join(conds, formatter)
}

func quote(src string) string {

	rst := strings.Replace(src, "`", "\\`", -1)

	return "`" + rst + "`"
}

func newBinlogReader(conn *Connection, serverID int32) (*replication.BinlogSyncer, error) {
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

	reader, err := newBinlogReader(conn, serverID)
	if err != nil {
		return nil, err
	}

	streamer, err := reader.StartSync(mysql.Position{binlogFile, uint32(binlogPos)})
	if err != nil {
		return nil, err
	}

	return streamer, nil
}

func newBinlogReaderByGTID(conn *Connection, GTID string, serverID int32) (*replication.BinlogStreamer, error) {
	reader, err := newBinlogReader(conn, serverID)
	if err != nil {
		return nil, err
	}

	gtidSet, err := mysql.ParseMysqlGTIDSet(GTID)
	if err != nil {
		return nil, err
	}

	streamer, err := reader.StartSyncGTID(gtidSet)
	if err != nil {
		return nil, err
	}

	return streamer, nil
}
