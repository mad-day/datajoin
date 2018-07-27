/*
   Copyright 2018 Simon Schmidt

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


package pqsrc

import "github.com/mad-day/datajoin/api"
import "database/sql"
import "bytes"
import "fmt"
import "github.com/lib/pq"
import "reflect"

type PqRowIter struct{
	Rows *sql.Rows
	Scanit []interface{}
}
func (p *PqRowIter) Close() error { return p.Rows.Close() }
func (p *PqRowIter) Next() bool { return p.Rows.Next() }
func (p *PqRowIter) Fetch() (api.Row,error) {
	r := make(api.Row,len(p.Scanit))
	err := p.Rows.Scan(p.Scanit...)
	if err!=nil { return nil,err }
	for i,ref := range p.Scanit {
		switch v := ref.(type) {
		case *int64:       r[i] = *v
		case *float64:     r[i] = *v
		case *bool:        r[i] = *v
		case *string:      r[i] = *v
		case *[]byte:      r[i] = *v
		case *interface{}: r[i] = *v
		}
	}
	return r,nil
}

func convertArray(x interface{}) interface{} {
	switch v := x.(type) {
	case []int64: return pq.Int64Array(v)
	case []float64: return pq.Float64Array(v)
	case []bool: return pq.BoolArray(v)
	case [][]byte: return pq.ByteaArray(v)
	case []string: return pq.StringArray(v)
	}
	return x
}
func clonearray(x []interface{}) (y []interface{}) {
	y = make([]interface{},len(x))
	for i,e := range x {
		y[i] = reflect.New(reflect.ValueOf(e).Type().Elem()).Interface()
	}
	return
}

type PqRowSource struct{
	Src       *sql.DB
	BaseQuery string
	Scanit    []interface{}
	ColNames  []string
	ColTypes  []reflect.Type
}
func NewRowSource(src *sql.DB,name string,cols []string,scanit []interface{}) *PqRowSource {
	b := new(bytes.Buffer)
	sel := "select"
	for _,col := range cols { fmt.Fprintf(b,"%s %q",sel,col); sel = "," }
	cts := make([]reflect.Type,len(scanit))
	for i,v := range scanit { cts[i] = reflect.TypeOf(v).Elem() }
	fmt.Fprintf(b," from %q",name)
	return &PqRowSource {src, b.String(), scanit, cols, cts}
}

func (p *PqRowSource) Names() []string { return p.ColNames }
func (p *PqRowSource) Types() []reflect.Type { return p.ColTypes }
func (p *PqRowSource) Lookup(specs ... interface{}) (api.RowIter,error) {
	b := new(bytes.Buffer)
	b.WriteString(p.BaseQuery)
	wher := "where"
	var res []interface{}
	for _,spec := range specs {
		switch v := spec.(type) {
		case api.Spec:
			res = append(res,convertArray(v.Values))
			fmt.Fprintf(b," %s %q = any($%d)",wher,v.Column,len(res))
			wher = "and"
		case api.SpecSingle:
			res = append(res,v.Value)
			fmt.Fprintf(b," %s %q = $%d",wher,v.Column,len(res))
			wher = "and"
		}
	}
	rows,err := p.Src.Query(b.String(),res...)
	if err!=nil { return nil,err }
	return &PqRowIter{rows,clonearray(p.Scanit)},nil
}


