package main

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
)

func makeUpdateSql(table string, idxField, idxValue, tbField, tbValue []string) string {
	var setClause string
	var whereClause string
	var limitClause string
	var tableClause string

	tableClause = quote(table, "`")
	setClause = makeSqlCondition(tbField, tbValue, "=", ", ")
	whereClause = makeWhereClause(idxField, idxValue)
	limitClause = "LIMIT 1"

	return fmt.Sprintf("UPDATE %s SET %s%s%s;", tableClause, setClause, whereClause, limitClause)
}

func makeInsertSql(table string, tbField, tbValue []string) string {
	var fldClause string
	var valClause string
	var tableClause string

	tableClause = quote(table, "`")

	fld := make([]string, len(tbField))
	val := make([]string, len(tbValue))

	for i := 0; i < len(tbField); i++ {
		fld[i] = quote(tbField[i], "`")
		val[i] = "\"" + mysql.Escape(tbValue[i]) + "\""
	}

	FileLog.Printf("table field: %v, value: %v\n", fld, val)

	fldClause = "(" + strings.Join(fld, ", ") + ")"
	valClause = "(" + strings.Join(val, ", ") + ")"

	return fmt.Sprintf("INSERT INTO %s %s VALUES %s;", tableClause, fldClause, valClause)
}

func makeDeleteSql(table string, idxField, idxValue []string) string {
	var whereClause string
	var limitClause string
	var tableClause string

	tableClause = quote(table, "`")

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
		conds[i] = fmt.Sprintf("%s%s%s", quote(k, "`"), operator, "\""+mysql.Escape(values[i])+"\"")
	}

	return strings.Join(conds, formatter)
}

func quote(src, quote string) string {
	rst := strings.Replace(src, quote, "\\"+quote, -1)
	return quote + rst + quote
}
