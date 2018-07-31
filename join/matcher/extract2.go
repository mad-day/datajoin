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
import "fmt"

type Predict bool
func (Predict) Resolved() bool { return true }
func (p Predict) String() string { if p { return "predict(true)" } ; return "predict(false)" }
func (Predict) IsNullable() bool { return false }
func (Predict) Type() sql.Type { return sql.Boolean }
func (p Predict) Eval(*sql.Context, sql.Row) (interface{}, error) { return bool(p),nil }
func (p Predict) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) { return f(p) }
func (Predict) Children() []sql.Expression { return nil }
var _ sql.Expression = Predict(false)

//type Known bool
//func (p Known) String() string { if p { return "known(true)" } ; return "known(false)" }

func predictsBool(expr sql.Expression,b bool) bool {
	p,ok := expr.(Predict)
	if bool(p)!=b || !ok { return false }
	return true
}

func MinimumTables(tabs []*query.AdHocTable,expr sql.Expression) (i int) {
	switch v := expr.(type) {
	case *expression.GetField:
		t := v.Table()
		i = len(tabs)
		for j,tab := range tabs {
			if tab.Name()==t {
				i = j+1
				break
			}
		}
	}
	for _,subex := range expr.Children() {
		j := MinimumTables(tabs,subex)
		if i<j { i = j }
	}
	return
}

type StaticHint uint
const (
	SHNone StaticHint = 1<<iota
	SHTrue
	SHFalse
	SHPass
)
func (sh StaticHint) Has(o StaticHint) bool {
	return (sh&o)==o
}

type AnnotExpr struct{
	sql.Expression
	Stat StaticHint
	
}
func defAnnotExpr(e sql.Expression) *AnnotExpr { return &AnnotExpr{Expression:e} }
func (a *AnnotExpr) String() string { return fmt.Sprintf("(%v)",a.Expression) }
func (a *AnnotExpr) Children() []sql.Expression { return []sql.Expression{a.Expression} }

func Wrap(e sql.Expression) (sql.Expression,error) { return defAnnotExpr(e),nil }
func Unwrap(e sql.Expression) (sql.Expression,error) { return e,nil }

/* self-service store */

/*
 expect:
	true  => pass if condition()
	false => pass unless condition()
*/
func iInspect(ts TableSet,expr sql.Expression,expect bool) (sql.Expression) {
	ae := expr.(*AnnotExpr)
	if ae.Stat.Has(SHTrue)  { return Predict(true)  }
	if ae.Stat.Has(SHFalse) { return Predict(false) }
	if ae.Stat.Has(SHPass)  { return Predict(expect) }
	
	//if CheckTables(ts,expr) {}
	
	switch v := expr.(*AnnotExpr).Expression.(type) {
	case *expression.And:
		left := iInspect(ts,v.Left,expect)
		right := iInspect(ts,v.Right,expect)
		if left==Predict(false) || right==Predict(false) {
			return Predict(false)
		}
		if left==Predict(true) { return right }
		if right==Predict(true) { return left }
		return expression.NewAnd(left,right)
	case *expression.Or:
		left := iInspect(ts,v.Left,expect)
		right := iInspect(ts,v.Right,expect)
		if left==Predict(true) || right==Predict(true) {
			return Predict(true)
		}
		if left==Predict(false) { return right }
		if right==Predict(false) { return left }
		return expression.NewAnd(left,right)
	case *expression.Not:
		inner := iInspect(ts,v.Child,!expect)
		switch p := inner.(type) {
		case Predict: return !p
		}
		return expression.NewNot(inner)
	}
	
	if CheckTables(expr,ts) {
		ae.Stat |= SHPass
		oe := ae.Expression
		//ae.Expression = Predict(expect)
		return oe
	}
	return Predict(expect)
}

func Inspect(ts TableSet,expr sql.Expression) (sql.Expression) {
	return iInspect(ts,expr,true)
}

