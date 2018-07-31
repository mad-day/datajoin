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
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "github.com/mad-day/datajoin/query"
import "github.com/mad-day/datajoin/join/matcher"
import "fmt"

func GetAll(node sql.Node) (mjs []*query.MultiJoin) {
	var f func(node sql.Node)
	f = func(node sql.Node) {
		switch v := node.(type) {
		case *query.MultiJoin:
			mjs = append(mjs,v)
		default:
			for _,c := range node.Children() { f(c) }
		}
	}
	f(node)
	return
}
func GetTables(mj *query.MultiJoin) (aht []*query.AdHocTable) {
	aht = make([]*query.AdHocTable,0,len(mj.Tables))
	for _,tab := range mj.Tables {
		t,ok := tab.(*query.AdHocTable)
		if !ok { panic("invalid table") }
		aht = append(aht,t)
	}
	return
}


type RealJoin struct{
	*query.Cookie
	Tables     []*query.AdHocTable
	Offsets    []int
	Dominated  []sql.Expression /* Dominated specifiers. */
	Equals     []sql.Expression /* Straight equals. Suitable for Hash Join. */
	Filters    []sql.Expression /* Other filters. */
	
	Prefilter  []sql.Expression /* Per-Table input filters. */
	Indexer    []matcher.FieldSpecs /* Index-Scan hints. */
	Indexer2   []*SpecBuilder /* Preprocessed version of .Indexer */
	Postfilter []sql.Expression
	Chunk     int
}
func (r *RealJoin) String() string {
	tp := sql.NewTreePrinter()
	tp.WriteNode("RealJoin")
	
	var ch1,ch2,ch3 string
	
	{
		filt := make([]string,len(r.Prefilter))
		for i,e := range r.Prefilter {
			if e==nil {
				filt[i] = fmt.Sprintf("%s : TRUE",r.Tables[i].Name())
			} else {
				filt[i] = fmt.Sprintf("%s : %v",r.Tables[i].Name(),e)
			}
		}
		exprs := sql.NewTreePrinter()
		exprs.WriteNode("Prefilter")
		exprs.WriteChildren(filt...)
		ch1 = exprs.String()
	}
	{
		filt := make([]string,len(r.Indexer))
		for i,e := range r.Indexer {
			filt[i] = fmt.Sprintf("%s : %v",r.Tables[i].Name(),e)
		}
		exprs := sql.NewTreePrinter()
		exprs.WriteNode("Indexer")
		exprs.WriteChildren(filt...)
		ch2 = exprs.String()
	}
	{
		filt := make([]string,len(r.Postfilter))
		for i,e := range r.Postfilter {
			if e==nil {
				filt[i] = fmt.Sprintf("%s : TRUE",r.Tables[i].Name())
			} else {
				filt[i] = fmt.Sprintf("%s : %v",r.Tables[i].Name(),e)
			}
		}
		exprs := sql.NewTreePrinter()
		exprs.WriteNode("Postfilter")
		exprs.WriteChildren(filt...)
		ch3 = exprs.String()
	}
	tp.WriteChildren(
		//fmt.Sprintf("Hash-Rules%s",expression.Tuple(r.Equals)),
		ch1,
		ch2,
		ch3,
	)
	
	return tp.String()
}
func NewRealJoin(mj *query.MultiJoin) (r *RealJoin) {
	r = new(RealJoin)
	r.Cookie = mj.Cookie
	r.Tables = GetTables(mj)
	for _,e := range mj.Filters {
		fmt.Println(matcher.MinimumTables(r.Tables,e),":",e)
		if matcher.IsDominated(e) {
			r.Dominated = append(r.Dominated,e)
		} else if matcher.IsEqual(e) {
			r.Equals = append(r.Equals,e)
		} else {
			r.Filters = append(r.Filters,e)
		}
	}
	flt := expression.JoinAnd(r.Filters...)
	if flt!=nil {
		flt,_ = flt.TransformUp(matcher.Wrap)
	} else {
		flt,_ = matcher.Predict(true).TransformUp(matcher.Wrap)
	}
	if len(mj.Filters)!=0 {
		a := expression.JoinAnd(mj.Filters...)
		fmt.Println(a)
		fmt.Println(matcher.Predict(true).TransformUp(matcher.Wrap))
	}
	
	r.Prefilter  = make([]sql.Expression,len(r.Tables))
	r.Indexer    = make([]matcher.FieldSpecs,len(r.Tables))
	r.Indexer2   = make([]*SpecBuilder,len(r.Tables))
	r.Offsets    = make([]int,len(r.Tables))
	r.Postfilter = make([]sql.Expression,len(r.Tables))
	pos := 0
	tsm := make(matcher.TableSetMap)
	for i,table := range r.Tables {
		tsl := len(table.Schema())
		pos += tsl
		tss := matcher.TableSetSimple(table.Name())
		filters := make([]sql.Expression,0,len(mj.Filters))
		tsm[table.Name()] = true
		for _,f := range mj.Filters {
			sr := matcher.GetSubrule(f,tss)
			if sr==nil { continue }
			sr,_ = sr.TransformUp(matcher.Indent(tsl-pos))
			filters = append(filters,sr)
		}
		
		r.Postfilter[i],_ = matcher.Inspect(tsm,flt).TransformUp(matcher.Unwrap)
		
		r.Prefilter[i] = expression.JoinAnd(filters...)		
		r.Indexer[i] = matcher.GetIndex(r.Tables[:i+1],mj.Filters)
		r.Indexer2[i] = NewSpecBuilder(r.Tables,r.Indexer[i])
		r.Offsets[i] = pos-tsl
	}
	
	
	
	return
}



