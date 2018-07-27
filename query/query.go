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

func joinJoins (node sql.Node) (sql.Node, error) {
	var left,right sql.Node
	var expr []sql.Expression
	switch v := node.(type) {
	case *plan.CrossJoin:
		left = v.Left
		right = v.Right
	case *plan.InnerJoin:
		left = v.Left
		right = v.Right
		expr = []sql.Expression{v.Cond}
	default:return node,nil
	}
	
	mj := new(MultiJoin)
	mj.Filters = expr
	
	if lmj,ok := left.(*MultiJoin); ok {
		mj.Tables = append(mj.Tables,lmj.Tables...)
		mj.Filters = append(mj.Filters,lmj.Filters...)
	} else {
		mj.Tables = append(mj.Tables,left)
	}
	
	if lmj,ok := right.(*MultiJoin); ok {
		sl := len(left.Schema())
		tf := indent(sl)
		mj.Tables = append(mj.Tables,lmj.Tables...)
		for _,f := range lmj.Filters {
			nf,_ := f.TransformUp(tf)
			mj.Filters = append(mj.Filters,nf)
		}
	} else {
		mj.Tables = append(mj.Tables,right)
	}
	
	return mj,nil
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

type DataContext struct{
	DS api.DataSource
}
func (dc DataContext) Parse(query string) (sql.Node,error) {
	db := mem.NewDatabase("public")
	
	mdb := &mdbObj{DB:db,DS:dc.DS}
	
	an := analyzer.NewDefault(sql.NewCatalog())
	an.Catalog.AddDatabase(db)
	an.CurrentDatabase = "public"
	
	ec := sql.NewEmptyContext()
	tree,err := parse.Parse(ec,query)
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(mdb.replaceAll))
	if err!=nil { return nil,err }
	
	fmt.Println(tree)
	
	tree,err = an.Analyze(ec,tree)
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(unAlias))
	if err!=nil { return nil,err }
	
	tree,err = tree.TransformUp(runOnEachSubquery(joinJoins))
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


type MultiJoin struct {
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

