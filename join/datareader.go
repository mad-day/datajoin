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


package join

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "github.com/mad-day/datajoin/api"
import "github.com/mad-day/datajoin/join/apis"
import "github.com/mad-day/datajoin/join/matcher"
import "github.com/mad-day/datajoin/query"
import "time"
import "github.com/spf13/cast"

type SpecType struct{
	Name   string
	New    func() interface{}
	Append func(array,elem interface{}) interface{}
	Conv   func(interface{}) interface{}
}

func genericConv(i interface{}) interface{} { return i }
func boolConv(i interface{}) interface{} {
	var b [2]bool
	for _,elem := range i.([]interface{}) {
		if cast.ToBool(elem) {
			b[1] = true
			if b[0] { return []bool{false,true} }
		} else {
			b[0] = true
			if b[1] { return []bool{false,true} }
		}
	}
	if b[1] { return []bool{true} }
	if b[0] { return []bool{false} }
	return nil
}

func strConv(i interface{}) interface{} {
	set := make(map[string]bool)
	for _,elem := range i.([]interface{}) {
		set[cast.ToString(elem)] = true
	}
	sm := make([]string,0,len(set))
	for k := range set { sm = append(sm,k) }
	return sm
}
func intConv(i interface{}) interface{} {
	set := make(map[int64]bool)
	for _,elem := range i.([]interface{}) {
		set[cast.ToInt64(elem)] = true
	}
	sm := make([]int64,0,len(set))
	for k := range set { sm = append(sm,k) }
	return sm
}
func fltConv(i interface{}) interface{} {
	set := make(map[float64]bool)
	for _,elem := range i.([]interface{}) {
		set[cast.ToFloat64(elem)] = true
	}
	sm := make([]float64,0,len(set))
	for k := range set { sm = append(sm,k) }
	return sm
}


func blobCast(i interface{}) []byte {
	if b,ok := i.([]byte); ok { return b }
	return []byte(cast.ToString(i))
}

func blobArray() interface{} { return [][]byte{} }
func timeArray() interface{} { return []time.Time{} }
func genericArray() interface{} { return []interface{}{} }

func blobAppend(array,elem interface{}) interface{} { return append(array.([][]byte),blobCast(elem)) }
func timeAppend(array,elem interface{}) interface{} { return append(array.([]time.Time),cast.ToTime(elem)) }
func genericAppend(array,elem interface{}) interface{} { return append(array.([]interface{}),elem) }

var (
	SpecInt = SpecType{"int64",genericArray,genericAppend,intConv}
	SpecFloat = SpecType{"float64",genericArray,genericAppend,fltConv}
	SpecBool = SpecType{"bool",genericArray,genericAppend,boolConv}
	SpecBlob = SpecType{"blob",blobArray,blobAppend,genericConv}
	SpecString = SpecType{"string",genericArray,genericAppend,strConv}
	SpecTimestamp = SpecType{"time.Time",timeArray,timeAppend,genericConv}
	specInvalid = SpecType{"<invalid>",genericArray,genericAppend,genericConv}
)

func sql2spec(t sql.Type) SpecType {
	switch {
	case sql.IsInteger(t): return SpecInt
	case sql.IsDecimal(t): return SpecFloat
	case t==sql.Text: return SpecString
	//case t==sql.JSON: return SpecString
	case t==sql.Boolean: return SpecBool
	case t==sql.Blob: return SpecBlob
	case t==sql.Timestamp: return SpecTimestamp
	case t==sql.Date: return SpecTimestamp
	}
	return specInvalid
}

func (s SpecType) String() string {
	return s.Name
}

type TargetedExpressions struct{
	Target int
	Exprs []sql.Expression
}
type SpecBuilder struct{
	Names []string
	Specs []SpecType
	Base []TargetedExpressions
	PerTable [][]TargetedExpressions
}
func NewSpecBuilder(tables []*query.AdHocTable, specs matcher.FieldSpecs) (sb *SpecBuilder) {
	sb = new(SpecBuilder)
	col := 0
	cm := make(map[string]int)
	ct := make(map[string]SpecType)
	getcol := func(s string) int {
		i,ok := cm[s]
		if ok { return i }
		i = col
		col++
		cm[s] = i
		return i
	}
	checktype := func(col string,exprs []sql.Expression) {
		if _,ok := ct[col] ; ok { return }
		for _,expr := range exprs {
			ct[col] = sql2spec(expr.Type())
			break
		}
	}
	for k,v := range specs[""] {
		sb.Base = append(sb.Base,TargetedExpressions{getcol(k),v})
		checktype(k,v)
	}
	sb.PerTable = make([][]TargetedExpressions,len(tables))
	for i,tab := range tables {
		var pt []TargetedExpressions
		for k,v := range specs[tab.Name()] {
			pt = append(pt,TargetedExpressions{getcol(k),v})
			checktype(k,v)
		}
		sb.PerTable[i] = pt
	}
	sb.Names = make([]string,col)
	sb.Specs = make([]SpecType,col)
	for k,v := range cm {
		sb.Names[v] = k
		s,ok := ct[k]
		if !ok { s = specInvalid }
		sb.Specs[v] = s
	}
	return
}

func (s *SpecBuilder) BaseSpecs(ctx *sql.Context) (res []interface{},e error ) {
	res = make([]interface{},len(s.Specs))
	for i,_ := range s.Specs {
		res[i] = s.Specs[i].New()
	}
	for _,t := range s.Base {
		array := res[t.Target]
		for _,expr := range t.Exprs {
			val,err := expr.Eval(ctx,nil)
			if err!=nil { e = err; return }
			array = s.Specs[t.Target].Append(array,val)
		}
		res[t.Target] = array
	}
	return
}
func (s *SpecBuilder) SpecsSetRows(ctx *sql.Context,tab int,rows []sql.Row,specs []interface{}) error {
	for _,t := range s.PerTable[tab] {
		array := specs[t.Target]
		for _,row := range rows {
			for _,expr := range t.Exprs {
				val,err := expr.Eval(ctx,row)
				if err!=nil { return err }
				array = s.Specs[t.Target].Append(array,val)
			}
		}
		specs[t.Target] = array
	}
	return nil
}
func (s *SpecBuilder) Lookup(src api.RowSource,specs []interface{}) (api.RowIter, error) {
	ts := make([]interface{},len(specs))
	for i,spec := range specs {
		ts[i] = api.Spec{s.Names[i],s.Specs[i].Conv(spec)}
	}
	return src.Lookup(ts...)
}

type iteration struct{
	*RealJoin
	blocks [][]sql.Row
	ctx *sql.Context
	chunk int
	endpt apis.BlockEndpoint
	onErrorDrop bool
}
func (r *iteration) recurse(tab int) error {
	if tab>=len(r.Tables) {
		return r.endpt.PassTabBlockRow(r.blocks)
	}
	specs,err := r.Indexer2[tab].BaseSpecs(r.ctx)
	if err!=nil { return err }
	for i,block := range r.blocks[:tab] {
		err := r.Indexer2[tab].SpecsSetRows(r.ctx,i,block,specs)
		if err!=nil { return err }
	}
	ri,err := r.Indexer2[tab].Lookup(r.Tables[tab].ItsSrc,specs)
	if err!=nil { return err }
	defer ri.Close()
	rows := r.blocks[tab][:0]
	for ri.Next() {
		row,err := ri.Fetch()
		if err!=nil {
			if r.onErrorDrop { continue }
			return err
		}
		if r.Prefilter[tab]!=nil {
			bol,_ := r.Prefilter[tab].Eval(r.ctx,sql.Row(row))
			if !cast.ToBool(bol) { continue }
		}
		rows = append(rows,sql.Row(row))
		if len(rows)>=r.chunk {
			r.blocks[tab] = rows
			rows = rows[:0]
			err = r.recurse(tab+1)
			if err!=nil { return err }
			if err := r.ctx.Err(); err!=nil { return err }
		}
	}
	if len(rows)!=0 {
		r.blocks[tab] = rows
		err = r.recurse(tab+1)
		if err!=nil { return err }
	}
	return nil
}


func (r *RealJoin) IterateOver(ctx *sql.Context,endpt apis.BlockEndpoint,chunk int) error {
	iter := &iteration{r,make([][]sql.Row,len(r.Tables)),ctx,chunk,endpt,false}
	return iter.recurse(0)
}


