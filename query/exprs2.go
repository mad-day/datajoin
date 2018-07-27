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

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "strings"

type Lowest []sql.Expression

var _ sql.Expression = (Lowest)(nil)

func(e Lowest) Resolved() bool { return true }
func(e Lowest) String() string {
	s := make([]string,len(e))
	for i,ee := range e { s[i] = ee.String() }
	return "lowest("+strings.Join(s,", ")+")"
}
func(e Lowest) Type() sql.Type {
	if len(e)==0 { return sql.Null }
	return e[0].Type()
}
func(e Lowest) IsNullable() bool {
	if len(e)==0 { return true }
	return e[0].IsNullable()
}
func(e Lowest) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if len(e)<2 { return true,nil }
	tp := e[0].Type()
	ref,err := e[0].Eval(ctx,row)
	if err!=nil { return nil,err }
	for _,ee := range e[1:] {
		oth,err := ee.Eval(ctx,row)
		if err!=nil { return nil,err }
		cmp,err := tp.Compare(ref,oth)
		if err!=nil { return nil,err }
		if cmp>0 {
			ref = oth
		}
	}
	return true,nil
}
func(e Lowest) TransformUp(tf sql.TransformExprFunc) (_ sql.Expression, err error) {
	ne := make(Lowest,len(e))
	for i,ee := range e {
		ne[i],err = tf(ee)
		if err!=nil { return }
	}
	return tf(ne)
}
func(e Lowest) Children() []sql.Expression { return e }

func NewLowest(exprs ...sql.Expression) (sql.Expression, error) { return Lowest(exprs),nil }

type Highest []sql.Expression

var _ sql.Expression = (Highest)(nil)

func(e Highest) Resolved() bool { return true }
func(e Highest) String() string {
	s := make([]string,len(e))
	for i,ee := range e { s[i] = ee.String() }
	return "highest("+strings.Join(s,", ")+")"
}
func(e Highest) Type() sql.Type {
	if len(e)==0 { return sql.Null }
	return e[0].Type()
}
func(e Highest) IsNullable() bool {
	if len(e)==0 { return true }
	return e[0].IsNullable()
}
func(e Highest) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if len(e)<2 { return true,nil }
	tp := e[0].Type()
	ref,err := e[0].Eval(ctx,row)
	if err!=nil { return nil,err }
	for _,ee := range e[1:] {
		oth,err := ee.Eval(ctx,row)
		if err!=nil { return nil,err }
		cmp,err := tp.Compare(ref,oth)
		if err!=nil { return nil,err }
		if cmp>0 {
			ref = oth
		}
	}
	return true,nil
}
func(e Highest) TransformUp(tf sql.TransformExprFunc) (_ sql.Expression, err error) {
	ne := make(Highest,len(e))
	for i,ee := range e {
		ne[i],err = tf(ee)
		if err!=nil { return }
	}
	return tf(ne)
}
func(e Highest) Children() []sql.Expression { return e }

func NewHighest(exprs ...sql.Expression) (sql.Expression, error) { return Highest(exprs),nil }


