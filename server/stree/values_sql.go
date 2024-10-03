package stree

import (
	"reflect"
	"unsafe"

        "zombiezen.com/go/sqlite"
)

func scalarSqlType(k reflect.Kind) string {
	switch k {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return "INT"
	case reflect.Float32, reflect.Float64:
		return "FLOAT"
	case reflect.String:
		return "TEXT"
	}
	return ""
}

func (t *SubjectTree[T]) setColParams() {
	if t.conn.vElemGob != nil {
		t.conn.vColType = "BLOB"
		t.conn.vElemSize = -1
		return
	}
	typ := reflect.TypeFor[T]()
	k := typ.Kind()
	if s := scalarSqlType(k); s != "" {
		t.conn.vColType = s
		return
	} else if k == reflect.Slice {
		typ := typ.Elem()
		s := scalarSqlType(typ.Kind())
		if s != "" && s != "TEXT" {
			t.conn.vColType = "BLOB"
			t.conn.vElemSize = int(typ.Size())
			return
		}
	}
	// we will need a proper Serializer
	t.conn.vElemGob = NewGobber[T](nil)
	t.storeGobDesc()
	t.conn.vColType = "BLOB"
	t.conn.vElemSize = -1
}

func colBytes(stmt *sqlite.Stmt, k int) []byte {
	buf := make([]byte, stmt.ColumnLen(k))
	stmt.ColumnBytes(k, buf)
	return buf
}

func (t *SubjectTree[T]) colToSlice(stmt *sqlite.Stmt, k int) *T {
	buf := colBytes(stmt, k)
	ptr := unsafe.Pointer(&buf[0])
	nelem := len(buf)/t.conn.vElemSize
	var v T
	switch any(v).(type) {
	case []bool:
		v = any(unsafe.Slice((*bool)(ptr), nelem)).(T)
	case []int:
		v = any(unsafe.Slice((*int)(ptr), nelem)).(T)
	case []int8:
		v = any(unsafe.Slice((*int8)(ptr), nelem)).(T)
	case []int16:
		v = any(unsafe.Slice((*int16)(ptr), nelem)).(T)
	case []int32:
		v = any(unsafe.Slice((*int32)(ptr), nelem)).(T)
	case []int64:
		v = any(unsafe.Slice((*int64)(ptr), nelem)).(T)
	case []uint:
		v = any(unsafe.Slice((*uint)(ptr), nelem)).(T)
	case []uint8:
		v = any(buf).(T)
	case []uint16:
		v = any(unsafe.Slice((*uint16)(ptr), nelem)).(T)
	case []uint32:
		v = any(unsafe.Slice((*uint32)(ptr), nelem)).(T)
	case []uint64:
		v = any(unsafe.Slice((*uint64)(ptr), nelem)).(T)
	case []uintptr:
		v = any(unsafe.Slice((*uintptr)(ptr), nelem)).(T)
	case []float32:
		v = any(unsafe.Slice((*float32)(ptr), nelem)).(T)
	case []float64:
		v = any(unsafe.Slice((*float64)(ptr), nelem)).(T)
	}
	return &v
}

func (t *SubjectTree[T]) colToValue(stmt *sqlite.Stmt, k int) *T {
	if t.conn.vElemGob != nil {
		return t.conn.vElemGob.Decode(colBytes(stmt, k))
	} else if t.conn.vElemSize > 0 {
		return t.colToSlice(stmt, k)
	}
	var r T
	switch any(r).(type) {
	case bool:
		r = any(stmt.ColumnBool(k)).(T)
	case int:
		r = any(int(stmt.ColumnInt64(k))).(T)
	case int8:
		r = any(int8(stmt.ColumnInt64(k))).(T)
	case int16:
		r = any(int16(stmt.ColumnInt64(k))).(T)
	case int32:
		r = any(int32(stmt.ColumnInt64(k))).(T)
	case int64:
		r = any(stmt.ColumnInt64(k)).(T)
	case uint:
		r = any(uint(stmt.ColumnInt(k))).(T)
	case uint8:
		r = any(uint8(stmt.ColumnInt64(k))).(T)
	case uint16:
		r = any(uint16(stmt.ColumnInt64(k))).(T)
	case uint32:
		r = any(uint32(stmt.ColumnInt64(k))).(T)
	case uint64:
		r = any(uint64(stmt.ColumnInt64(k))).(T)
	case uintptr:
		r = any(uintptr(stmt.ColumnInt64(k))).(T)
	case float32:
		r = any(float32(stmt.ColumnFloat(k))).(T)
	case float64:
		r = any(stmt.ColumnFloat(k)).(T)
	case string:
		r = any(stmt.ColumnText(k)).(T)
	default:
		// panic?
	}
	return &r
}

func (t *SubjectTree[T]) bindSlice(stmt *sqlite.Stmt, k int, value T) {
	v := reflect.ValueOf(value)
	ptr := (*byte)(v.UnsafePointer())
	stmt.BindBytes(k, unsafe.Slice(ptr, v.Len()*t.conn.vElemSize))
}

func (t *SubjectTree[T]) bindValue(stmt *sqlite.Stmt, k int, value T) {
	if t.conn.vElemGob != nil {
		stmt.BindBytes(k, t.conn.vElemGob.Encode(&value))
		return
	} else if t.conn.vElemSize > 0 {
		t.bindSlice(stmt, k, value)
		return
	}
	switch any(value).(type) {
	case bool:
		stmt.BindBool(k, any(value).(bool))
	case int:
		stmt.BindInt64(k, int64(any(value).(int)))
	case int8:
		stmt.BindInt64(k, int64(any(value).(int8)))
	case int16:
		stmt.BindInt64(k, int64(any(value).(int16)))
	case int32:
		stmt.BindInt64(k, int64(any(value).(int32)))
	case int64:
		stmt.BindInt64(k, any(value).(int64))
	case uint:
		stmt.BindInt64(k, int64(any(value).(uint)))
	case uint8:
		stmt.BindInt64(k, int64(any(value).(uint8)))
	case uint16:
		stmt.BindInt64(k, int64(any(value).(uint16)))
	case uint32:
		stmt.BindInt64(k, int64(any(value).(uint32)))
	case uint64:
		stmt.BindInt64(k, int64(any(value).(uint64)))
	case uintptr:
		stmt.BindInt64(k, int64(any(value).(uintptr)))
	case float32:
		stmt.BindFloat(k, float64(any(value).(float32)))
	case float64:
		stmt.BindFloat(k, any(value).(float64))
	case string:
		stmt.BindText(k, any(value).(string))
	default:
		// panic?
	}
}
