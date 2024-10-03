package stree

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

        "zombiezen.com/go/sqlite"
        "zombiezen.com/go/sqlite/sqlitex"
)

const (
	opStore = iota
	opFind
	opMatch
)

var (
	errStop = errors.New("Stop")
)

type void = struct{}

type sqlConn[T any] struct {
	*sqlite.Conn
	dbFile     string
	strCollate map[int]int
	tabSubj    map[int]void
	maxTabSubj int
	iterStmt   *sqlite.Stmt
	vColType   string // INT, FLOAT, TEXT, BLOB
	vElemSize  int    // if value is a slice
	vElemGob   *Gobber[T]
}

var (
	queriesStr = make(map[int]string)
	queriesIdx = make(map[int]string)
	insertions = make(map[int]string)

	queriesSubjFull = make(map[int]string)
	queriesSubjWild = make(map[int]string)
	insertionsSubj  = make(map[int]string)
	deletesSubj     = make(map[int]string)
)

func sqlStmtStr(n int, action string) string {
	if n < 1 {
		panic(fmt.Errorf("sqlStmtStr: n=%d", n))
	}
	var b strings.Builder
	b.WriteString("with q(k, v) as (values")
	sep := " "
	for range n {
		b.WriteString(sep)
		b.WriteString("(?, ?)")
		sep = ", "
	}
	b.WriteString(") ")
	b.WriteString(action)
	b.WriteString(";")
	return b.String()
}

func queryStr(n int) string {
	if s, ok := queriesStr[n]; ok {
		return s
	}
	s := sqlStmtStr(n, "select k, i from q, Strings where t = v")
	queriesStr[n] = s
	return s
}

func queryIdx(n int) string {
	if s, ok := queriesIdx[n]; ok {
		return s
	}
	s := sqlStmtStr(n, "select k, t from q, Strings where i = v")
	queriesIdx[n] = s
	return s
}

func insertStr(n int) string {
	if s, ok := insertions[n]; ok {
		return s
	}
	s := sqlStmtStr(n, "insert into Strings(t) select v from q returning (select k from q where t = v), i")
	insertions[n] = s
	return s
}

func insertSubject(n int) string {
	if s, ok := insertionsSubj[n]; ok {
		return s
	}
	if n < 1 {
		panic(fmt.Errorf("insertSubject: n=%d", n))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "insert or replace into Subject%d values (", n)
	for k := range n+1 {
		if k > 0 {
			b.WriteString(", ")
		}
		b.WriteString("?")
	}
	b.WriteString(");")
	s := b.String()
	insertionsSubj[n] = s
	return s
}

func querySubjectFull(n int) string {
	if s, ok := queriesSubjFull[n]; ok {
		return s
	}
	s := selOrDelSubject(n, "select v")
	queriesSubjFull[n] = s
	return s
}

func deleteSubject(n int) string {
	if s, ok := deletesSubj[n]; ok {
		return s
	}
	s := selOrDelSubject(n, "delete")
	deletesSubj[n] = s
	return s
}

func selOrDelSubject(n int, action string) string {
	if n < 1 {
		panic(fmt.Errorf("insOrDelSubject: n=%d", n))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s from Subject%d where (", action, n)
	for k := range n {
		if k > 0 {
			b.WriteString(" and ")
		}
		fmt.Fprintf(&b, "t%d = ?", k+1)
	}
	b.WriteString(");")
	return b.String()
}

func querySubjectWild(n int) string {
	if s, ok := queriesSubjWild[n]; ok {
		return s
	}
	if n < 1 {
		panic(fmt.Errorf("querySubjectWild: n=%d", n))
	}
	var b strings.Builder
	b.WriteString("with q(")
	for k := range n {
		if k > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "x%d", k+1)
	}
	b.WriteString(") as (values (")
	for k := range n {
		if k > 0 {
			b.WriteString(", ")
		}
		b.WriteString("?")
	}
	fmt.Fprintf(&b, "))\n    select Subject%d.* from q, Subject%d where\n    ", n, n)
	for k := range n {
		if k > 0 {
			b.WriteString(" and")
			if k%4 == 0 {
				b.WriteString("\n    ")
			} else {
				b.WriteString(" ")
			}
		}
		fmt.Fprintf(&b, "(x%d = 0 or x%d = t%d)", k+1, k+1, k+1)
	}
	b.WriteString(";")
	s := b.String()
	queriesSubjWild[n] = s
	return s
}

var createStrings = `
create table if not exists Strings(
    i integer primary key,
    t text unique on conflict ignore);`

var scanTables = `
select name from sqlite_schema where type = 'table';`

func (t *SubjectTree[T]) sqlInit(cfg *Config) {
        if t == nil {
		return
	}
	var err error
	var dbConn *sqlite.Conn
	var dbFile string
	if cfg == nil || cfg.DBPath == "" {
		dbFile = "MEMORY"
		dbConn, err = sqlite.OpenConn(dbFile,
			sqlite.OpenReadWrite|sqlite.OpenCreate|sqlite.OpenMemory)
	} else {
		dbFile = cfg.DBPath
		if strings.Contains(cfg.DBPath, "*") {
			instId := fmt.Sprintf("%016x", rand.Uint64())
			dbFile = strings.Replace(cfg.DBPath, "*", instId, 1)
		}
		dbConn, err = sqlite.OpenConn(dbFile)
	}
	if err == nil {
		err = sqlitex.ExecuteTransient(dbConn, createStrings, nil)
	}
	if err != nil {
		panic(fmt.Errorf("sqlInit createStrings: %w", err))
	}
	tabSubj := make(map[int]void, 16)
	var maxTabSubj int
	err = sqlitex.ExecuteTransient(dbConn, scanTables, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			name := stmt.ColumnText(0)
			if !strings.HasPrefix(name, "Subject") {
				return nil
			}
			if n, err := strconv.Atoi(name[7:]); err == nil {
				tabSubj[n] = void{}
				if n > maxTabSubj {
					maxTabSubj = n
				}
			}
			return nil
		},
	})
	if err != nil {
		panic(fmt.Errorf("sqlInit scanTables: %w", err))
	}
	var vElemGob *Gobber[T]
	err = sqlitex.ExecuteTransient(dbConn, "select desc from GobDesc;", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			if vElemGob != nil {
				return fmt.Errorf("sqlInit: multiple GobDesc's")
			}
			vElemGob = NewGobber[T](colBytes(stmt, 0))
			return vElemGob.Error()
		},
	})
	if err == nil || strings.Contains(err.Error(), " no such table:") {
		// grr: no way to use errors.Is(...)
		// pass
	} else if err != nil {
		panic(fmt.Errorf("sqlInit read GobDesc: %w", err))
	}
	size := 0
	for n, _ := range tabSubj {
		query := fmt.Sprintf("select count(*) from Subject%d;", n)
		err = sqlitex.ExecuteTransient(dbConn, query, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				size += stmt.ColumnInt(0)
				return nil
			},
		})
		if err != nil {
			panic(fmt.Errorf("sqlInit count Subjects: %w", err))
		}
	}
	err = dbConn.CreateFunction("StrOrd", &sqlite.FunctionImpl{
		NArgs: 1,
		Scalar: func(ctx sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
			return sqlite.IntegerValue(int64(t.conn.strCollate[args[0].Int()])), nil
		},
		Deterministic: true,
	})
	if err != nil {
		panic(fmt.Errorf("sqlInit CreateFunction: %w", err))
	}
	t.size = size
	t.conn = &sqlConn[T]{
		Conn:       dbConn,
		tabSubj:    tabSubj,
		maxTabSubj: maxTabSubj,
		vElemGob:    vElemGob,
	}
	t.setColParams()
}

func (t *SubjectTree[T]) sqlClose() error {
	err := t.conn.Close()
	t.conn = nil
	return err
}

func (t *SubjectTree[T]) storeGobDesc() {
	if t == nil || t.conn == nil || t.conn.vElemGob == nil {
		return
	}
	err := sqlitex.ExecuteScript(t.conn.Conn, `
		drop table if exists GobDesc;
		create table GobDesc (desc BLOB);`, nil)
	if err == nil {
		err = sqlitex.ExecuteTransient(t.conn.Conn,
			"insert into GobDesc values (?);",
			&sqlitex.ExecOptions{Args: []any{t.conn.vElemGob.Desc()}})
	}
	if err != nil {
		panic(fmt.Errorf("storeGobDesc: %w", err))
	}
}

func (t *SubjectTree[T]) sqlIdxToStr(idx []int, strs []string) []string {
        if t == nil || t.conn == nil {
		return nil
	}
	stmt := t.conn.Prep(queryIdx(len(idx)))
	k := 1
	for kp, i := range idx {
		strs = append(strs, "")
		stmt.BindInt64(k, int64(kp))
		stmt.BindInt64(k+1, int64(i))
		k += 2
	}
	for {
		if hasRow, err := stmt.Step(); err != nil {
			panic(err)
		} else if !hasRow {
			break
		}
		k, v := stmt.ColumnInt(0), stmt.ColumnText(1)
		strs[k] = v
	}
	stmt.Reset()
	stmt.ClearBindings()
	return strs
}

func subjectToTokens(subject []byte, tokens []string) ([]string, int) {
	ntok := 0
	for start := 0; start < len(subject); {
		end := bytes.IndexByte(subject[start:], tsep)
		if end < 0 {
			end = len(subject)
		} else {
			end += start
		}
		token := subject[start:end]
		// n.b. fwc is only a wildcard at the end of a subject.
		if len(token) != 1 || (token[0] != pwc && (token[0] != fwc || end < len(subject))) {
			ntok++
		}
		tokens = append(tokens, string(token))
		start = end + 1
	}
	return tokens, ntok
}

func (t *SubjectTree[T]) subjectToIdx(op int, subject []byte, idx []int) []int {
        if t == nil || t.conn == nil {
		return nil
	}
	var _tokens [16]string
	tokens, ntok := subjectToTokens(subject, _tokens[:0])
	if op != opMatch && len(tokens) != ntok { // only opMatch allows wildcards
		return nil
	}
	if ntok == 0 { // only wildcards
		for range len(tokens) {
			idx = append(idx, 0)
		}
		return idx
	}

	stmt := t.conn.Prep(queryStr(ntok))
	k := 1
	for kt, token := range tokens {
		if len(token) != 1 || (token[0] != pwc && token[0] != fwc) {
			idx = append(idx, -1)
			stmt.BindInt64(k, int64(kt))
			stmt.BindText(k+1, token)
			k += 2
		} else {
			idx = append(idx, 0)
		}
	}
	for {
		if hasRow, err := stmt.Step(); err != nil {
			panic(err)
		} else if !hasRow {
			break
		}
		k, v := stmt.ColumnInt(0), stmt.ColumnInt(1)
		idx[k] = v
		ntok--
	}
	stmt.Reset()
	stmt.ClearBindings()
	if ntok == 0 {
		return idx
	} else if op != opStore {
		return nil
	}
	t.conn.strCollate = nil
	stmt = t.conn.Prep(insertStr(ntok))
	k = 1
	for kt, i := range idx {
		if i < 0 { // one that we missed
			stmt.BindInt64(k, int64(kt))
			stmt.BindText(k+1, tokens[kt])
			k += 2
		}
	}
	for {
		if hasRow, err := stmt.Step(); err != nil {
			panic(err)
		} else if !hasRow {
			break
		}
		k, i := stmt.ColumnInt(0), stmt.ColumnInt(1)
		idx[k] = i
		ntok--
	}
	stmt.Reset()
	stmt.ClearBindings()
	if ntok == 0 {
		return idx
	}
	// one more pass, there may have been duplicates in the updates
	stmt = t.conn.Prep(queryStr(ntok))
	k = 1
	for kt, i := range idx {
		if i < 0 {
			stmt.BindInt64(k, int64(kt))
			stmt.BindText(k+1, tokens[kt])
			k += 2
		}
	}
	for {
		if hasRow, err := stmt.Step(); err != nil {
			panic(err)
		} else if !hasRow {
			break
		}
		k, i := stmt.ColumnInt(0), stmt.ColumnInt(1)
		idx[k] = i
		ntok--
	}
	stmt.Reset()
	stmt.ClearBindings()
	if ntok != 0 {
		fmt.Printf("idx = %v\n", idx)
		fmt.Printf("tokens = %v\n", tokens)
		panic(fmt.Errorf("subjectToIdx: ntok=%d", ntok))
	}
	return idx
}

// ensureSubject is short to encourage inlining
func (t *SubjectTree[T]) ensureSubject(n int) {
        if t == nil || t.conn == nil {
		return
	}
	if _, ok := t.conn.tabSubj[n]; !ok {
		t._ensureSubject(n)
	}
}

func (t *SubjectTree[T]) _ensureSubject(n int) string {
	if n < 1 {
		panic(fmt.Errorf("_ensureSubject: n=%d", n))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "create table if not exists Subject%d(", n)
	for k := range n {
		if k > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "t%d int", k+1)
	}
	fmt.Fprintf(&b, ", v %s, primary key (", t.conn.vColType)
	for k := range n {
		if k > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "t%d", k+1)
	}
	b.WriteString("));")
	s := b.String()
	err := sqlitex.ExecuteTransient(t.conn.Conn, s, nil)
	if err != nil {
		panic(fmt.Errorf("_ensureSubject %d: %w", n, err))
	}
	if n > t.conn.maxTabSubj {
		t.conn.maxTabSubj = n
		if t.conn.iterStmt != nil { // it's stale now
			t.conn.iterStmt.Finalize()
			t.conn.iterStmt = nil
		}
	}
	t.conn.tabSubj[n] = void{}
	return s
}

func (t *SubjectTree[T]) sqlEmpty() {
        if t == nil || t.conn == nil {
		return
	}
	must := func(action string) {
		err := sqlitex.ExecuteTransient(t.conn.Conn, action, nil)
		if err != nil {
			panic(fmt.Errorf("sqlEmpty: %w", err))
		}
	}
	must("drop table if exists Strings;")
	for n, _ := range t.conn.tabSubj {
		must(fmt.Sprintf("drop table if exists Subject%d;", n))
	}
	clear(t.conn.tabSubj)
	t.conn.maxTabSubj = 0
	t.conn.strCollate = nil
	if t.conn.iterStmt != nil {
		t.conn.iterStmt.Finalize()
		t.conn.iterStmt = nil
	}
	must(createStrings)
}

func (t *SubjectTree[T]) sqlInsert(subject []byte, value T) (*T, bool) {
	var _idx [16]int
	idx := t.subjectToIdx(opStore, subject, _idx[:0])
	if idx == nil { // malformed subject
		return nil, false
	}
	old := t.sqlFindByIdx(idx)

	stmt := t.conn.Prep(insertSubject(len(idx)))
	for k, i := range idx {
		stmt.BindInt64(k+1, int64(i))
	}
	t.bindValue(stmt, len(idx)+1, value)
	if _, err := stmt.Step(); err != nil {
		panic(err)
	}
	stmt.Reset()
	stmt.ClearBindings()

	if old == nil {
		t.size++
		return nil, false
	}
	return old, true
}

func (t *SubjectTree[T]) sqlFindByIdx(idx []int) (pValue *T) {
	ntok := len(idx)
	if ntok == 0 {
		return
	}
	t.ensureSubject(ntok)
	stmt := t.conn.Prep(querySubjectFull(ntok))
	for k, i := range idx {
		stmt.BindInt64(k+1, int64(i))
	}
	for {
		if hasRow, err := stmt.Step(); err != nil {
			panic(err)
		} else if !hasRow {
			break
		} else if pValue != nil {
			panic("multiple subject values")
		}
		pValue = t.colToValue(stmt, 0)
	}
	stmt.Reset()
	stmt.ClearBindings()
	return
}

func (t *SubjectTree[T]) sqlFind(subject []byte) (*T, bool) {
	var _idx [16]int
	idx := t.subjectToIdx(opFind, subject, _idx[:0])
	pValue := t.sqlFindByIdx(idx)
	return pValue, (pValue != nil)
}

func (t *SubjectTree[T]) sqlDelete(subject []byte) (*T, bool) {
	var _idx [16]int
	idx := t.subjectToIdx(opFind, subject, _idx[:0])
	old := t.sqlFindByIdx(idx)
	if old == nil {
		return nil, false
	}
	stmt := t.conn.Prep(deleteSubject(len(idx)))
	for k, i := range idx {
		stmt.BindInt64(k+1, int64(i))
	}
	if _, err := stmt.Step(); err != nil {
		panic(err)
	}
	stmt.Reset()
	stmt.ClearBindings()
	t.size--
	return old, true
}

func (t *SubjectTree[T]) sqlMatch(filter []byte, cb func(subject []byte, val *T)) {
	var _idx [16]int
	idx := t.subjectToIdx(opMatch, filter, _idx[:0])
	if idx == nil {
		return
	}
	ntLast := len(idx)
	if filter[len(filter)-1] == '>' {
		ntLast = t.conn.maxTabSubj
	}
	for {
		if _, ok := t.conn.tabSubj[len(idx)]; ok {
			t.sqlMatchIdx(idx, cb)
		}
		if len(idx) >= ntLast {
			break
		}
		idx = append(idx, 0)
	}
}

func (t *SubjectTree[T]) sqlMatchIdx(idx []int, cb func(subject []byte, val *T)) {
	stmt := t.conn.Prep(querySubjectWild(len(idx)))
	for k, i := range idx {
		stmt.BindInt64(k+1, int64(i))
	}
	var _midx [16]int
	var _tokens [16]string
	midx, tokens := _midx[:0], _tokens[:0]
	for {
		if hasRow, err := stmt.Step(); err != nil {
			panic(err)
		} else if !hasRow {
			break
		}
		midx = midx[:0]
		for k := range idx {
			midx = append(midx, stmt.ColumnInt(k))
		}
		tokens = t.sqlIdxToStr(midx, tokens[:0])
		pVal := t.colToValue(stmt, len(idx))
		cb([]byte(strings.Join(tokens, ".")), pVal)
	}
	stmt.Reset()
	stmt.ClearBindings()
}

func (t *SubjectTree[T]) sqlSortStrings() {
	t.conn.strCollate = make(map[int]int)
	t.conn.strCollate[0] = 0 // empty string
	stmt := t.conn.Prep("select i from Strings order by t;")
	k := 1
	for {
		if hasRow, err := stmt.Step(); err != nil {
			panic(err)
		} else if !hasRow {
			break
		}
		t.conn.strCollate[stmt.ColumnInt(0)] = k
		k++
	}
	stmt.Reset()
}

func (t *SubjectTree[T]) sqlIterQuery() string {
	var b strings.Builder
	b.WriteString("with data(")
	pfx := ""
	for k := range t.conn.maxTabSubj {
		fmt.Fprintf(&b, "%st%d", pfx, k+1)
		pfx = ", "
	}
	b.WriteString(", v) as (")
	pfx = "\n    "
	for nt := 1; nt <= t.conn.maxTabSubj; nt++ {
		if _, ok := t.conn.tabSubj[nt]; !ok {
			continue
		}
		b.WriteString(pfx)
		pfx = "\n    union "
		b.WriteString("select ")
		kpfx := ""
		for k := 1; k <= nt; k++ {
			fmt.Fprintf(&b, "%st%d", kpfx, k)
			kpfx = ", "
		}
		for range t.conn.maxTabSubj-nt {
			b.WriteString(", 0")
		}
		fmt.Fprintf(&b, ", v from Subject%d", nt)
	}
	b.WriteString("\n) select * from data order by ")
	pfx = ""
	for k := range t.conn.maxTabSubj {
		fmt.Fprintf(&b, "%sStrOrd(t%d)", pfx, k+1)
		pfx = ", "
	}
	b.WriteString(";")
	return b.String()
}

func (t *SubjectTree[T]) sqlIter(cb func(subject []byte, val *T) bool) {
	if t.conn.strCollate == nil {
		t.sqlSortStrings()
	}
	if t.conn.iterStmt == nil {
		t.conn.iterStmt = t.conn.Prep(t.sqlIterQuery())
	}
	stmt := t.conn.iterStmt

	var _idx [16]int
	var _tokens [16]string
	idx, tokens := _idx[:0], _tokens[:0]
	for {
		if hasRow, err := stmt.Step(); err != nil {
			panic(err)
		} else if !hasRow {
			break
		}
		idx = idx[:0]
		for k := range t.conn.maxTabSubj {
			i := stmt.ColumnInt(k)
			if i == 0 {
				break
			}
			idx = append(idx, i)
		}
		tokens = t.sqlIdxToStr(idx, tokens[:0])
		pVal := t.colToValue(stmt, t.conn.maxTabSubj)
		if !cb([]byte(strings.Join(tokens, ".")), pVal) {
			break
		}
	}
	stmt.Reset()
}
