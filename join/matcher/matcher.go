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


package matcher

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "github.com/mad-day/datajoin/query"
import "errors"
import "fmt"

func Indent(i int) sql.TransformExprFunc {
	return func(expr sql.Expression) (sql.Expression, error) {
		if gf,ok := expr.(*expression.GetField); ok {
			//fmt.Println(gf,"change",gf.Index(),"to",gf.Index()+i)
			return gf.WithIndex(gf.Index()+i),nil
		}
		return expr,nil
	}
}

var eNone = errors.New("is none!")

type NoneVar struct{}
func(NoneVar) Resolved() bool { return false }
func(NoneVar) String() string { return "<NONE>" }
func(NoneVar) Type() sql.Type { return sql.Null }
func(NoneVar) IsNullable() bool { return true }
func(NoneVar) Eval(*sql.Context, sql.Row) (interface{}, error) { return nil,eNone }
func(NoneVar) TransformUp(tef sql.TransformExprFunc) (sql.Expression, error) { return tef(NoneVar{}) }
func(NoneVar) Children() []sql.Expression { return nil }

type TableSet interface{ Has(s string) bool }
type TableSetMap map[string]bool
func (t TableSetMap) Has(s string) bool { return t[s] }
type TableSetSimple string
func (t TableSetSimple) Has(s string) bool { return string(t)==s }

func TableSetFor(tabs []*query.AdHocTable) TableSet {
	switch len(tabs) {
	case 0: return TableSetMap(nil)
	case 1: return TableSetSimple(tabs[0].Name())
	}
	ts := make(TableSetMap)
	for _,t := range tabs {
		ts[t.Name()] = true
	}
	return ts
}

func IsFieldOf(expr sql.Expression,table string) (string,bool) {
	switch v := expr.(type) {
	case *expression.GetField:
		if v.Table()!=table { return "",false }
		return v.Name(),true
	}
	return "",false
}

func CheckTables(expr sql.Expression,ts TableSet) bool {
	switch v := expr.(type) {
	case *expression.GetField:
		if !ts.Has(v.Table()) { return false }
	}
	for _,subex := range expr.Children() {
		if !CheckTables(subex,ts) { return false }
	}
	return true
}

func IsSatisfiedEqual(expr sql.Expression,ts TableSet) bool {
	var empty TableSet = TableSetMap(nil)
	switch expr.(type) {
	case *expression.Equals,query.Equal:
		for _,subex := range expr.Children() {
			if CheckTables(subex,empty) { continue }
			if CheckTables(subex,ts) { return true }
		}
	}
	return false
}
func GetValueForHash(expr sql.Expression,ts TableSet) sql.Expression {
	var empty TableSet = TableSetMap(nil)
	switch expr.(type) {
	case *expression.Equals,query.Equal:
		for _,subex := range expr.Children() {
			if CheckTables(subex,empty) { continue }
			if CheckTables(subex,ts) { return subex }
		}
	}
	return nil
}

func IsDominated(expr sql.Expression) bool {
	switch v := expr.(type) {
	case *expression.Equals:
		if CheckTables(v.Left (),TableSetMap(nil)) { return true }
		if CheckTables(v.Right(),TableSetMap(nil)) { return true }
	case query.Equal:
		for _,vv := range v {
			if CheckTables(vv,TableSetMap(nil)) { return true }
		}
	}
	return false
}

/* Conservative equal(...) */
func IsEqual(expr sql.Expression) bool {
	switch expr.(type) {
	case *expression.Equals,query.Equal: return true
	}
	return false
}
func IsEqualAny(expr sql.Expression) bool {
	switch expr.(type) {
	case *expression.Equals,query.Equal,*expression.In: return true
	}
	return false
}

func GetSubrule(expr sql.Expression,ts TableSet) sql.Expression {
	nexpr,err := expr.TransformUp(func(expr sql.Expression) (sql.Expression,error) {
		switch v := expr.(type) {
		case *expression.GetField:
			if !ts.Has(v.Table()) { return NoneVar{},nil }
		case query.Equal:
			nq := make(query.Equal,0,len(v))
			for _,elem := range v {
				if _,ok := elem.(NoneVar); ok { continue }
				nq = append(nq,elem)
			}
			if len(nq)<2 { return NoneVar{},nil }
			return nq,nil
		}
		for _,elem := range expr.Children() {
			if _,ok := elem.(NoneVar); ok { return NoneVar{},nil }
		}
		
		return expr,nil
	})
	if err!=nil { return nil }
	if _,ok := nexpr.(NoneVar); ok { return nil }
	return nexpr
}

type FieldSpecsTable map[string][]sql.Expression
func (fst FieldSpecsTable) isEmpty() bool {
	return len(fst)==0
}
func (fst FieldSpecsTable) stringFor(tab string) string {
	tp := sql.NewTreePrinter()
	if tab=="" {
		tp.WriteNode("Specifiers")
	} else {
		tp.WriteNode("Specifiers FROM %s",tab)
	}
	n := make([]string,0,len(fst))
	for k,v := range fst {
		if len(v)==0 { continue }
		n = append(n,fmt.Sprintf("%s IN %v",k,expression.Tuple(v)))
	}
	tp.WriteChildren(n...)
	return tp.String()
}
type FieldSpecs map[string]FieldSpecsTable
func (fs FieldSpecs) String() string {
	tp := sql.NewTreePrinter()
	tp.WriteNode("FieldSpecs")
	n := make([]string,0,len(fs))
	if fst,ok := fs[""]; ok {
		if !fst.isEmpty() {
			n = append(n,fst.stringFor(""))
		}
	}
	for k,fst := range fs {
		if k=="" { continue }
		if !fst.isEmpty() {
			n = append(n,fst.stringFor(k))
		}
	}
	tp.WriteChildren(n...)
	
	return tp.String()
}


func GetIndex(tabs []*query.AdHocTable,exprs []sql.Expression) (fs FieldSpecs) {
	fs = make(FieldSpecs)
	//var tab TableSetSimple
	table := tabs[len(tabs)-1].Name()
	fs[""] = make(FieldSpecsTable)
	for _,tab := range tabs[:len(tabs)-1] {
		fs[tab.Name()] = make(FieldSpecsTable)
	}
	equality := func(expr sql.Expression,fields... string) {
		if CheckTables(expr,TableSetMap(nil)) {
			v := fs[""]
			for _,field := range fields {
				v[field] = append(v[field],expr)
			}
			return
		}
		pos := 0
		for _,tab := range tabs[:len(tabs)-1] {
			tsl := len(tab.Schema())
			pos += tsl
			if !CheckTables(expr,TableSetSimple(tab.Name())) { continue }
			expr,_ = expr.TransformUp(Indent(tsl-pos))
			v := fs[tab.Name()]
			for _,field := range fields {
				v[field] = append(v[field],expr)
			}
		}
	}
	dominatedEquality := func(expr sql.Expression,fields... string) {
		if CheckTables(expr,TableSetMap(nil)) {
			v := fs[""]
			for _,field := range fields {
				v[field] = append(v[field],expr)
			}
		}
	}
	for _,expr := range exprs {
		curEq := equality
		if IsDominated(expr) { curEq = dominatedEquality }
		switch v := expr.(type) {
		case *expression.Equals:
			if f,ok := IsFieldOf(v.Left(),table); ok {
				curEq(v.Right(),f)
			} else if f,ok := IsFieldOf(v.Right(),table); ok {
				curEq(v.Left(),f)
			}
		case query.Equal:
			var fns []string
			var rest []sql.Expression
			for _,elem := range v {
				f,ok := IsFieldOf(elem,table)
				if ok {
					fns = append(fns,f)
				} else {
					rest = append(rest,elem)
				}
			}
			for _,elem := range rest {
				curEq(elem,fns...)
			}
		case *expression.In:
			if f,ok := IsFieldOf(v.Left(),table); ok {
				if tpl,ok := v.Right().(expression.Tuple); ok {
					for _,elem := range tpl {
						curEq(elem,f)
					}
				}
			}
		}
	}
	return
}

