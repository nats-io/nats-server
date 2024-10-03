package stree

import (
	//"bytes"
	"fmt"
	//"strconv"
	"strings"
	"sync"

        "zombiezen.com/go/sqlite"
        //"zombiezen.com/go/sqlite/sqlitex"
)

type stmtClass int

const (
	queryStr stmtClass = iota
	queryIdx
	insertStr
	querySubjectFull
	querySubjectWild
	insertSubject
	deleteSubject
	sortStrings

	numStmtClasses
)

type cacheByClass[T any] struct {
	sync.RWMutex
	cm [numStmtClasses]map[int]T
}

// stmtCache (sql statement strings) is shared among sqlConn instances
var (
	stmtCache cacheByClass[string]
	stmtFns   = []func(int) string{
		getQueryStr,
		getQueryIdx,
		getInsertStr,
		getQuerySubjectFull,
		getQuerySubjectWild,
		getInsertSubject,
		getDeleteSubject,
		getSortStrings,
	}
)

// a pool of identical prepared sqlite.Stmt's
type prepPool struct {
	sync.Mutex
	stmts []*sqlite.Stmt
}

// an sqlite.Stmt drawn from a prepPool
type prepStmt struct {
	pool *prepPool
	*sqlite.Stmt
}

func (c *cacheByClass[T]) fetchOrGen(cl stmtClass, n int, genFn func(int) T) T {
	c.RLock()
	s, ok := c.cm[cl][n]
	c.RUnlock()
	if ok {
		return s
	}
	c.Lock()
	defer c.Unlock()
	sm := c.cm[cl]
	if s, ok = sm[n]; !ok {
		s = genFn(n)
		if sm == nil {
			sm = make(map[int]T)
			c.cm[cl] = sm
		}
		sm[n] = s
	}
	return s
}

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

func getQueryStr(n int) string {
	return stmtCache.fetchOrGen(queryStr, n, func(n int) string {
		return sqlStmtStr(n, "select k, i from q, Strings where t = v")
	})
}

func getQueryIdx(n int) string {
	return stmtCache.fetchOrGen(queryIdx, n, func(n int) string {
		return sqlStmtStr(n, "select k, t from q, Strings where i = v")
	})
}

func getInsertStr(n int) string {
	return stmtCache.fetchOrGen(insertStr, n, func(n int) string {
		return sqlStmtStr(n,
			"insert into Strings(t) select v from q returning (select k from q where t = v), i")
	})
}

func getInsertSubject(n int) string {
	return stmtCache.fetchOrGen(insertSubject, n, func(n int) string {
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
		return b.String()
	})
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

func getQuerySubjectFull(n int) string {
	return stmtCache.fetchOrGen(querySubjectFull, n, func(n int) string {
		return selOrDelSubject(n, "select v")
	})
}

func getDeleteSubject(n int) string {
	return stmtCache.fetchOrGen(deleteSubject, n, func(n int) string {
		return selOrDelSubject(n, "delete")
	})
}

func getSortStrings(n int) string {
	return "select i from Strings order by t;"
}

func getQuerySubjectWild(n int) string {
	return stmtCache.fetchOrGen(querySubjectWild, n, func(n int) string {
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
		return b.String()
	})
}

func (sc *sqlConn[T]) prepStmt(cl stmtClass, n int) *prepStmt {
	pool := sc.stmts.fetchOrGen(cl, n, func(n int) *prepPool {
		return new(prepPool)
	})
	pool.Lock()
	if n := len(pool.stmts)-1; n >= 0 {
		ret := prepStmt{pool: pool, Stmt: pool.stmts[n]}
		pool.stmts = pool.stmts[:n]
		pool.Unlock()
		return &ret
	}
	pool.Unlock()
	stmt, _, err := sc.PrepareTransient(stmtFns[cl](n))
	if err != nil {
		panic(fmt.Errorf("PrepareTransient: %w", err))
	}
	return &prepStmt{pool: pool, Stmt: stmt}
}

func (stmt *prepStmt) mustStep() bool {
	if hasRow, err := stmt.Step(); err != nil {
		panic(err)
	} else if hasRow {
		return true
	}
	stmt.Reset()
	stmt.ClearBindings()
	if stmt.pool != nil {
		stmt.pool.Lock()
		stmt.pool.stmts = append(stmt.pool.stmts, stmt.Stmt)
		stmt.pool.Unlock()
	}
	return false
}

func (stmt *prepStmt) mustExec() {
	if stmt.mustStep() {
		panic("hasRow")
	}
}

func (sc *sqlConn[T]) finalize() {
	sc.stmts.Lock()
	for cl, sm := range sc.stmts.cm {
		sc.stmts.cm[cl] = nil
		for _, pool := range sm {
			pool.Lock()
			for k, stmt := range pool.stmts {
				pool.stmts[k] = nil
				stmt.Finalize()
			}
			pool.stmts = nil
			pool.Unlock()
		}
	}
	sc.stmts.Unlock()
	if stmt := sc.iterStmt; stmt != nil {
		sc.iterStmt = nil
		stmt.Finalize()
	}
}
