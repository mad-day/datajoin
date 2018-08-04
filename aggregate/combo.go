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

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "github.com/emirpasic/gods/maps/treemap"
import "github.com/spf13/cast"
import "fmt"

type Filter struct{
	Aggr sql.Aggregation
	Expr sql.Expression
}
var _ sql.Aggregation = (*Filter)(nil)
func (a *Filter) Resolved() (ok bool) { return a.Aggr.Resolved()&&a.Expr.Resolved() }
func (a *Filter) String() string { return fmt.Sprintf("filter(%v,%v)",a.Aggr,a.Expr) }
func (a *Filter) Type() sql.Type { return a.Aggr.Type() }
func (a *Filter) IsNullable() bool { return true }
func (a *Filter) TransformUp(st sql.TransformExprFunc) (sql.Expression, error) {
	b,err := a.Aggr.TransformUp(st)
	if err!=nil { return nil,err }
	e,err := a.Expr.TransformUp(st)
	if err!=nil { return nil,err }
	c,ok := b.(sql.Aggregation)
	if !ok { return nil,fmt.Errorf("expected aggregation, got %v",ok) }
	return st(&Filter{c,e})
}
func (a *Filter) Children() []sql.Expression { return []sql.Expression{a.Aggr,a.Expr} }
func (a *Filter) NewBuffer() sql.Row {
	return sql.Row{nil}
}
func (a *Filter) Update(ctx *sql.Context, buffer, row sql.Row) error {
	ok,err := a.Expr.Eval(ctx,row)
	if err!=nil { return err }
	
	/* Only continue with those lines, we really want. */
	if !cast.ToBool(ok) { return nil }
	if buffer[0]==nil { buffer[0] = a.Aggr.NewBuffer() }
	return a.Aggr.Update(ctx,buffer[0].(sql.Row),row)
}
func (a *Filter) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	if buffer[0]!=nil {
		return a.Aggr.Merge(ctx,buffer[0].(sql.Row),partial[0].(sql.Row))
	}
	buffer[0] = partial[0]
	return nil
}
func (a *Filter) Eval(_ *sql.Context, buffer sql.Row) (interface{}, error) { return buffer[0],nil }

func NewFilter(args ...sql.Expression) (sql.Expression, error) {
	if len(args)!=2 { return nil,sql.ErrInvalidArgumentNumber.New(2, len(args)) }
	aggr,ok := args[0].(sql.Aggregation)
	if !ok { return nil,fmt.Errorf("expected aggregation, got %v",args[0]) }
	return &Filter{aggr,args[1]},nil
}


func comparator(t sql.Type) func(a, b interface{}) int {
	return func(a, b interface{}) int {
		i,err := t.Compare(a,b)
		if err!=nil { return 0 }
		return i
	}
}

type GroupBy struct{
	Expr sql.Expression
	Aggr sql.Aggregation
	Maximum uint64
}
var _ sql.Aggregation = (*GroupBy)(nil)
func (a *GroupBy) Resolved() (ok bool) { return a.Aggr.Resolved()&&a.Expr.Resolved() }
func (a *GroupBy) String() string { return fmt.Sprintf("group_by(%v,%v,%v)",a.Aggr,a.Expr,a.Maximum) }
func (a *GroupBy) Type() sql.Type { return sql.Array(a.Aggr.Type()) }
func (a *GroupBy) IsNullable() bool { return true }
func (a *GroupBy) TransformUp(st sql.TransformExprFunc) (sql.Expression, error) {
	e,err := a.Expr.TransformUp(st)
	if err!=nil { return nil,err }
	b,err := a.Aggr.TransformUp(st)
	if err!=nil { return nil,err }
	c,ok := b.(sql.Aggregation)
	if !ok { return nil,fmt.Errorf("expected aggregation, got %v",ok) }
	return st(&GroupBy{e,c,a.Maximum})
}
func (a *GroupBy) Children() []sql.Expression { return []sql.Expression{a.Expr,a.Aggr} }
func (a *GroupBy) NewBuffer() sql.Row {
	return sql.Row{treemap.NewWith(comparator(a.Aggr.Type())),new(uint64)}
}
func (a *GroupBy) Update(ctx *sql.Context, buffer, row sql.Row) error {
	key,err := a.Expr.Eval(ctx,row)
	if err!=nil { return err }
	m := buffer[0].(*treemap.Map)
	i := buffer[1].(*uint64)
	v,ok := m.Get(key)
	if !ok {
		if *i >= a.Maximum { return nil }
		*i++
		v = a.Aggr.NewBuffer()
		m.Put(key,v)
	}
	return a.Aggr.Update(ctx,v.(sql.Row),row)
}
func (a *GroupBy) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	m := buffer[0].(*treemap.Map)
	i := buffer[1].(*uint64)
	oth := partial[0].(*treemap.Map).Iterator()
	oth.Begin()
	for oth.Next() {
		v,ok := m.Get(oth.Key())
		if !ok {
			if *i >= a.Maximum { break }
			*i++
			m.Put(oth.Key(),oth.Value())
		}
		err := a.Aggr.Merge(ctx,v.(sql.Row),oth.Value().(sql.Row))
		if err!=nil { return err }
	}
	return nil
}
func (a *GroupBy) Eval(ctx *sql.Context, buffer sql.Row) (_ interface{}, e error) {
	r := buffer[0].(*treemap.Map).Values()
	for i,elem := range r {
		r[i],e = a.Aggr.Eval(ctx,elem.(sql.Row))
		if e!=nil { return }
	}
	return r,nil
}
func NewGroupBy(args ...sql.Expression) (sql.Expression, error) {
	i := uint64(1<<11)
	switch len(args) {
	case 3:
		lit,ok := args[2].(*expression.Literal)
		if !ok { return nil,fmt.Errorf("expected integer literal, got %v",args[2]) }
		var err error
		i,err = cast.ToUint64E(lit.Value())
		if err!=nil { return nil,err }
	case 2:
	default:
		return nil,sql.ErrInvalidArgumentNumber.New(2, len(args))
	}
	aggr,ok := args[1].(sql.Aggregation)
	if !ok { return nil,fmt.Errorf("expected aggregation, got %v",args[1]) }
	
	return &GroupBy{args[0],aggr,i},nil
}

