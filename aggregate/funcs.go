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
import "github.com/spf13/cast"

type Dict []sql.Expression
var _ sql.Expression = Dict(nil)
func (a Dict) Resolved() (ok bool) {
	ok = true
	for _,y := range a { ok = ok || y.Resolved() }
	return
}
func (a Dict) String() string { return "dict"+expression.Tuple(a).String() }
//Implements sql.Expression
func (a Dict) Type() sql.Type { return sql.JSON }
//Implements sql.Expression
func (a Dict) IsNullable() bool { return false }
//Implements sql.Expression
func (a Dict) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	r := make(map[string]interface{})
	var s string
	for i,elem := range a {
		val,err := elem.Eval(ctx,row)
		if err!=nil { return nil,err }
		switch (i&1) {
		case 0:
			s = cast.ToString(val)
		case 1:
			r[s] = val
		}
	}
	return r,nil
}
//Implements sql.Expression
func (a Dict) TransformUp(f sql.TransformExprFunc) (_ sql.Expression, e error) {
	b := make(Dict,len(a))
	for i,c := range a { b[i],e = c.TransformUp(f); if e!=nil { return } }
	return f(b)
}
//Implements sql.Expression
func (a Dict) Children() []sql.Expression { return a }

func NewDict(exprs ...sql.Expression) (sql.Expression, error) {
	exprs = Clone(exprs)
	return Dict(exprs),nil
}


type Array []sql.Expression
var _ sql.Expression = Array(nil)
func (a Array) Resolved() (ok bool) {
	ok = true
	for _,y := range a { ok = ok || y.Resolved() }
	return
}
func (a Array) String() string { return "array"+expression.Tuple(a).String() }
//Implements sql.Expression
func (a Array) Type() sql.Type { return sql.JSON }
//Implements sql.Expression
func (a Array) IsNullable() bool { return false }
//Implements sql.Expression
func (a Array) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	r := make([]interface{},len(a))
	for i,elem := range a {
		val,err := elem.Eval(ctx,row)
		if err!=nil { return nil,err }
		r[i] = val
	}
	return r,nil
}
//Implements sql.Expression
func (a Array) TransformUp(f sql.TransformExprFunc) (_ sql.Expression, e error) {
	b := make(Array,len(a))
	for i,c := range a { b[i],e = c.TransformUp(f); if e!=nil { return } }
	return f(b)
}
//Implements sql.Expression
func (a Array) Children() []sql.Expression { return a }

func NewArray(exprs ...sql.Expression) (sql.Expression, error) {
	exprs = Clone(exprs)
	return Array(exprs),nil
}
