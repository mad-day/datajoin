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


package query

import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression/function"
import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-mysql-server.v0/sql/parse"
import "gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
import "gopkg.in/src-d/go-mysql-server.v0/sql/plan"
import "gopkg.in/src-d/go-mysql-server.v0/mem"
import "github.com/mad-day/datajoin/api"
import "fmt"
import "strings"
import "reflect"

func runOnEachSubquery(f sql.TransformNodeFunc) (r sql.TransformNodeFunc) {
	r = func(node sql.Node) (sql.Node, error) {
		switch v := node.(type) {
		case *plan.SubqueryAlias:
			up,err := v.Child.TransformUp(r)
			if err!=nil { return nil,err }
			return plan.NewSubqueryAlias(v.Name(),up),nil
		}
		return f(node)
	}
	return
}
func runOnEachSubqueryExpr(e sql.TransformExprFunc) sql.TransformNodeFunc {
	return func(node sql.Node) (sql.Node, error) {
		switch v := node.(type) {
		case *plan.SubqueryAlias:
			up,err := v.Child.TransformExpressionsUp(e)
			if err!=nil { return nil,err }
			return plan.NewSubqueryAlias(v.Name(),up),nil
		}
		return node,nil
	}
}
func indent(i int) sql.TransformExprFunc {
	return func(expr sql.Expression) (sql.Expression, error) {
		if gf,ok := expr.(*expression.GetField); ok {
			return gf.WithIndex(gf.Index()+i),nil
		}
		return expr,nil
	}
}

func unAlias (node sql.Node) (sql.Node, error) {
	switch v := node.(type) {
	case *plan.TableAlias:
		return v.Child,nil
	}
	return node,nil
}

func simplifyJoins (node sql.Node) (sql.Node, error) {
	switch v := node.(type) {
	case *plan.InnerJoin: return plan.NewFilter(v.Cond,plan.NewCrossJoin(v.Left,v.Right)),nil
	}
	return node,nil
}

func unifyJoins (node sql.Node) (sql.Node, error) {
	switch v := node.(type) {
	case *AdHocTable:
		mj := new(MultiJoin)
		mj.Cookie = new(Cookie)
		mj.Tables = []sql.Node{node}
		return mj,nil
	case *plan.Filter:
		omj,ok := v.Child.(*MultiJoin)
		if !ok { return node,nil }
		mj := new(MultiJoin)
		mj.Cookie = omj.Cookie
		mj.Tables = omj.Tables
		mj.Filters = append(omj.Filters,v.Expression)
		return mj,nil
	case *plan.CrossJoin:
		lmj,lok := v.Left.(*MultiJoin)
		rmj,rok := v.Right.(*MultiJoin)
		if !(lok&&rok) { return node,nil }
		mj := new(MultiJoin)
		mj.Cookie = new(Cookie)
		mj.Tables = append(mj.Tables,lmj.Tables...)
		mj.Filters = append(mj.Filters,lmj.Filters...)
		sl := len(lmj.Schema())
		tf := indent(sl)
		mj.Tables = append(mj.Tables,rmj.Tables...)
		for _,f := range rmj.Filters {
			nf,_ := f.TransformUp(tf)
			mj.Filters = append(mj.Filters,nf)
		}
		return mj,nil
	default: return node,nil
	}
	
	return node,nil
}

func pullOffFilters (node sql.Node) (sql.Node, error) {
	switch v := node.(type) {
	case *MultiJoin:
		if len(v.Filters)==0 { break }
		cond := expression.JoinAnd(v.Filters...)
		return plan.NewFilter(cond,&MultiJoin{Cookie:v.Cookie,Tables:v.Tables}),nil
	case *plan.Filter:
		v2,ok := v.Child.(*plan.Filter)
		if !ok { break }
		return plan.NewFilter(expression.NewAnd(v.Expression,v2.Expression),v2.Child),nil
	}
	return node,nil
}
func traverseAnd(expr sql.Expression) (exprs []sql.Expression) {
	var tf func(expr sql.Expression)
	tf = func(expr sql.Expression) {
		switch v := expr.(type) {
		case *expression.And:
			tf(v.Left)
			tf(v.Right)
		default:
			exprs = append(exprs,expr)
		}
	}
	tf(expr)
	return
}
func pushDownFilters (node sql.Node) (sql.Node, error) {
	switch v := node.(type) {
	case *plan.Filter:
		mj,ok := v.Child.(*MultiJoin)
		if !ok { break }
		return plan.NewFilter(
			v.Expression,
			&MultiJoin{
				Cookie:mj.Cookie,
				Tables:mj.Tables,
				Filters:traverseAnd(v.Expression),
			},
		),nil
	}
	return node,nil
}

type mdbObj struct{
	DB *mem.Database
	DS api.DataSource
	nn int
}
func (m *mdbObj) replaceAll(node sql.Node) (sql.Node, error) {
	switch v := node.(type){
	case *plan.UnresolvedTable:
		tab := m.DS.GetSource(v.Name)
		if tab==nil { return nil,fmt.Errorf("No such table %q",v.Name) }
		m.nn++
		nn := fmt.Sprintf("sampler_%d",m.nn)
		
		m.DB.AddTable(nn,NewAdHocTable(tab,nn))
		return plan.NewTableAlias(v.Name,plan.NewUnresolvedTable(nn)),nil
	case *plan.TableAlias:
		if ta,ok := v.Child.(*plan.TableAlias); ok {
			return plan.NewTableAlias(v.Name(),ta.Child),nil
		}
	}
	return node,nil
}

var ebad_mj = fmt.Errorf("improper MultiJoin")
var efound_mj = fmt.Errorf("found MultiJoin")
func findMultiJoin(expr sql.Node) (sql.Node, error) {
	switch v := expr.(type) {
	case *MultiJoin:
		for _,c := range v.Tables {
			if _,ok := c.(*AdHocTable) ; !ok { return nil,ebad_mj }
		}
		return nil,efound_mj
	}
	return expr,nil
}
func emplaceMultiJoin(expr sql.Node) (sql.Node, error) {
	switch expr.(type) {
	case *AdHocTable:
		return &MultiJoin{Cookie:new(Cookie),Tables:[]sql.Node{expr}},nil
	}
	return expr,nil
}

type DataContext struct{
	DS api.DataSource
}
func (dc DataContext) Parse(query string) (sql.Node,error) {
	return dc.ParseEx(query,nil)
}
func (dc DataContext) ParseEx(query string, costomFuncs sql.Functions) (sql.Node,error) {
	db := mem.NewDatabase("public")
	
	mdb := &mdbObj{DB:db,DS:dc.DS}
	
	an := analyzer.NewDefault(sql.NewCatalog())
	an.Catalog.AddDatabase(db)
	an.CurrentDatabase = "public"
	for k,v := range function.Defaults {
		an.Catalog.RegisterFunction(k,v)
	}
	for k,v := range costomFuncs {
		an.Catalog.RegisterFunction(k,v)
	}
	an.Catalog.RegisterFunction("equal",sql.FunctionN(NewEqual))
	an.Catalog.RegisterFunction("each",sql.FunctionN(NewEach))
	an.Catalog.RegisterFunction("anyof",sql.FunctionN(NewAny))
	an.Catalog.RegisterFunction("lowest",sql.FunctionN(NewLowest))
	an.Catalog.RegisterFunction("highest",sql.FunctionN(NewHighest))
	
	ec := sql.NewEmptyContext()
	tree,err := parse.Parse(ec,query)
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(mdb.replaceAll))
	if err!=nil { return nil,err }
	
	tree,err = an.Analyze(ec,tree)
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformExpressionsUp(convertSpecialOne)
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubqueryExpr(convertSpecialOne))
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(unAlias))
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(simplifyJoins))
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(unifyJoins))
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(pullOffFilters))
	if err!=nil { return nil,err }
	
	_,err = tree.TransformUp(runOnEachSubquery(findMultiJoin))
	if err==nil {
		tree,err = tree.TransformUp(runOnEachSubquery(emplaceMultiJoin))
	} else if err==efound_mj {
	} else { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(pushDownFilters))
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformExpressionsUp(validateSpecial)
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubqueryExpr(validateSpecial))
	if err!=nil { return nil,err }
	
	return tree,nil
}

type AdHocTable struct{
	ItsSrc  api.RowSource
	ItsName string
}
func NewAdHocTable(src api.RowSource,name string) (t *AdHocTable) {
	t = new(AdHocTable)
	t.ItsSrc = src
	t.ItsName = name
	return t
}

func reftype2sqltype(t reflect.Type) sql.Type {
	switch t.String() {
	case "int64": return sql.Int64
	case "float64": return sql.Float64
	case "time.Time": return sql.Timestamp
	case "string": return sql.Text
	case "bool": return sql.Boolean
	case "[]uint8": return sql.Blob
	}
	//if t.Kind() == reflect.Array {
	//	return sql.Array(reftype2sqltype(t.Elem()))
	//}
	return sql.JSON
}
func (t *AdHocTable) Schema() (s sql.Schema) {
	nms := t.ItsSrc.Names()
	tps := t.ItsSrc.Types()
	cols := make([]sql.Column,len(nms))
	s = make(sql.Schema,len(nms))
	
	for i := range cols {
		cols[i].Name = nms[i]
		cols[i].Type = reftype2sqltype(tps[i])
		cols[i].Source = t.ItsName
	}
	for i := range cols {
		s[i] = &cols[i]
	}
	return
}
func (t *AdHocTable) Name() string { return t.ItsName }
func (t *AdHocTable) Resolved() bool { return true }
func (t *AdHocTable) String() string { return fmt.Sprintf("Sampler %s",t.ItsName) }
func (t *AdHocTable) Children() []sql.Node { return nil }
func (t *AdHocTable) RowIter(*sql.Context) (sql.RowIter, error) { return nil,fmt.Errorf("In 100 years we're dead!") }
func (t *AdHocTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) { return f(t)}
func (t *AdHocTable) TransformExpressionsUp(sql.TransformExprFunc) (sql.Node, error) { return t,nil }

type Cookie struct{}
type MultiJoin struct {
	*Cookie
	Tables  []sql.Node
	Filters []sql.Expression
}
func (m *MultiJoin) Resolved() bool { return true }
func (m *MultiJoin) String() string {
	pr := sql.NewTreePrinter()
	var childs = make([]string, len(m.Tables))
	for i, child := range m.Tables {
		childs[i] = child.String()
	}
	var exprs = make([]string, len(m.Filters))
	for i, expr := range m.Filters {
		exprs[i] = "("+expr.String()+")"
	}
	_ = pr.WriteNode("MultiJoin %s", strings.Join(exprs, " AND "))
	_ = pr.WriteChildren(childs...)
	return pr.String()
}
func (m *MultiJoin) Children() []sql.Node { return nil }
func (m *MultiJoin) RowIter(*sql.Context) (sql.RowIter, error) {
	return nil,fmt.Errorf("In 100 years we're dead!")
}
func (m *MultiJoin) Schema() (s sql.Schema) {
	for _,child := range m.Tables {
		s = append(s,child.Schema()...)
	}
	return
}

func (m *MultiJoin) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(m)
}
func (m *MultiJoin) TransformExpressionsUp(sql.TransformExprFunc) (sql.Node, error) {
	return m,nil
}
func (m *MultiJoin) TransformExpressions(sql.TransformExprFunc) (sql.Node, error) {
	return m,nil
}
var _ sql.Node = (*MultiJoin)(nil)

