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


package aggregate

//func(...sql.Expression) (sql.Expression, error)
import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "github.com/spf13/cast"
import "fmt"

type AsList struct{
	expr sql.Expression
	max_count int
	resultType sql.Type
}
var _ sql.Aggregation = (*AsList)(nil)
func (a *AsList) Resolved() (ok bool) { return a.expr.Resolved() }
func (a *AsList) String() string { return fmt.Sprintf("as_list(%v,%v)",a.expr,a.max_count) }
func (a *AsList) Type() sql.Type { return a.resultType }
func (a *AsList) IsNullable() bool { return false }
func (a *AsList) TransformUp(st sql.TransformExprFunc) (sql.Expression, error) {
	n,err := a.expr.TransformUp(st)
	if err!=nil { return nil,err }
	return st(&AsList{n,a.max_count,sql.Array(n.Type())})
}
func (a *AsList) Children() []sql.Expression { return []sql.Expression{a.expr} }
func (a *AsList) NewBuffer() sql.Row {
	cnt := a.max_count
	if cnt > (1<<14) { cnt = 1<<16 }
	return sql.Row{make([]interface{},0,cnt)}
}
func (a *AsList) Update(ctx *sql.Context, buffer, row sql.Row) error {
	arr := buffer[0].([]interface{})
	if len(arr) >= a.max_count { return nil }
	val,err := a.expr.Eval(ctx,row)
	if err!=nil { return err }
	arr = append(arr,val)
	buffer[0] = arr
	return nil
}
func (a *AsList) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	arr := buffer[0].([]interface{})
	if len(arr) >= a.max_count { return nil }
	arr2 := partial[0].([]interface{})
	rest := a.max_count - len(arr)
	if len(arr2)>rest { arr2 = arr2[:rest] }
	arr = append(arr,arr2...)
	buffer[0] = arr
	return nil
}

func (a *AsList) Eval(_ *sql.Context, buffer sql.Row) (interface{}, error) { return buffer[0],nil }

func NewAsList(args ...sql.Expression) (sql.Expression, error) {
	a := new(AsList)
	a.max_count = 1<<11
	switch len(args) {
	case 2:
		lit,ok := args[1].(*expression.Literal)
		if !ok { return nil,fmt.Errorf("expected integer literal, got %v",args[1]) }
		var err error
		a.max_count,err = cast.ToIntE(lit.Value())
		if err!=nil { return nil,err }
	case 1:
		a.expr = args[0]
		a.resultType = sql.Array(a.expr.Type())
	default:
		return nil,sql.ErrInvalidArgumentNumber.New(2, len(args))
	}
	return a,nil
}

type First struct{
	Expr sql.Expression
}
var _ sql.Aggregation = (*First)(nil)
func (a *First) Resolved() (ok bool) { return a.Expr.Resolved() }
func (a *First) String() string { return fmt.Sprintf("first(%v)",a.Expr) }
func (a *First) Type() sql.Type { return a.Expr.Type() }
func (a *First) IsNullable() bool { return a.Expr.IsNullable() }
func (a *First) TransformUp(st sql.TransformExprFunc) (sql.Expression, error) {
	n,err := a.Expr.TransformUp(st)
	if err!=nil { return nil,err }
	return st(&First{n})
}
func (a *First) Children() []sql.Expression { return []sql.Expression{a.Expr} }
func (a *First) NewBuffer() sql.Row {
	return sql.Row{false,nil}
}
func (a *First) Update(ctx *sql.Context, buffer, row sql.Row) error {
	if buffer[0].(bool) { return nil }
	val,err := a.Expr.Eval(ctx,row)
	if err!=nil { return err }
	buffer[0] = true
	buffer[1] = val
	return nil
}
func (a *First) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	if buffer[0].(bool) { return nil }
	buffer[0] = partial[0]
	buffer[1] = partial[1]
	return nil
}
func (a *First) Eval(_ *sql.Context, buffer sql.Row) (interface{}, error) { return buffer[1],nil }

func NewFirst(i sql.Expression) sql.Expression { return &First{i} }

func toAggregation(i sql.Expression) sql.Aggregation {
	if a,ok := i.(sql.Aggregation) ; ok { return a }
	return &First{i}
}

type Last struct{
	Expr sql.Expression
}
var _ sql.Aggregation = (*Last)(nil)
func (a *Last) Resolved() (ok bool) { return a.Expr.Resolved() }
func (a *Last) String() string { return fmt.Sprintf("last(%v)",a.Expr) }
func (a *Last) Type() sql.Type { return a.Expr.Type() }
func (a *Last) IsNullable() bool { return a.Expr.IsNullable() }
func (a *Last) TransformUp(st sql.TransformExprFunc) (sql.Expression, error) {
	n,err := a.Expr.TransformUp(st)
	if err!=nil { return nil,err }
	return st(&Last{n})
}
func (a *Last) Children() []sql.Expression { return []sql.Expression{a.Expr} }
func (a *Last) NewBuffer() sql.Row {
	return sql.Row{nil}
}
func (a *Last) Update(ctx *sql.Context, buffer, row sql.Row) error {
	val,err := a.Expr.Eval(ctx,row)
	if err!=nil { return err }
	buffer[0] = val
	return nil
}
func (a *Last) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	buffer[0] = partial[0]
	return nil
}
func (a *Last) Eval(_ *sql.Context, buffer sql.Row) (interface{}, error) { return buffer[0],nil }

func NewLast(i sql.Expression) sql.Expression { return &Last{i} }

