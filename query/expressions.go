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
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "strings"
import "fmt"

type invalidExpression interface{
	isInvalid()
}

type Equal []sql.Expression

var _ sql.Expression = (Equal)(nil)

func(e Equal) Resolved() bool { return true }
func(e Equal) String() string {
	s := make([]string,len(e))
	for i,ee := range e { s[i] = ee.String() }
	return "equal("+strings.Join(s,", ")+")"
}
func(e Equal) Type() sql.Type { return sql.Boolean }
func(e Equal) IsNullable() bool { return false }
func(e Equal) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if len(e)<2 { return true,nil }
	tp := e[0].Type()
	ref,err := e[0].Eval(ctx,row)
	if err!=nil { return nil,err }
	for _,ee := range e[1:] {
		oth,err := ee.Eval(ctx,row)
		if err!=nil { return nil,err }
		cmp,err := tp.Compare(ref,oth)
		if err!=nil { return nil,err }
		if cmp!=0 { return false,nil }
	}
	return true,nil
}
func(e Equal) TransformUp(tf sql.TransformExprFunc) (_ sql.Expression, err error) {
	ne := make(Equal,len(e))
	for i,ee := range e {
		ne[i],err = tf(ee)
		if err!=nil { return }
	}
	return tf(ne)
}
func(e Equal) Children() []sql.Expression { return e }

func NewEqual(exprs ...sql.Expression) (sql.Expression, error) { return Equal(exprs),nil }


type Each []sql.Expression

var _ sql.Expression = (Each)(nil)

func(e Each) Resolved() bool { return true }
func(e Each) String() string {
	s := make([]string,len(e))
	for i,ee := range e { s[i] = ee.String() }
	return "each("+strings.Join(s,", ")+")"
}
func(e Each) Type() sql.Type {
	if len(e)==0 { return sql.Null }
	return e[0].Type()
}
func(e Each) IsNullable() bool {
	if len(e)==0 { return true }
	return e[0].IsNullable()
}
func(e Each) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if len(e)==0 { return nil,nil }
	return e[0].Eval(ctx,row)
}
func(e Each) TransformUp(tf sql.TransformExprFunc) (_ sql.Expression, err error) {
	ne := make(Each,len(e))
	for i,ee := range e {
		ne[i],err = tf(ee)
		if err!=nil { return }
	}
	return tf(ne)
}
func(e Each) Children() []sql.Expression { return e }
func(e Each) isInvalid() {}

func NewEach(exprs ...sql.Expression) (sql.Expression, error) { return Each(exprs),nil }

type Any []sql.Expression

var _ sql.Expression = (Any)(nil)

func(e Any) Resolved() bool { return true }
func(e Any) String() string {
	s := make([]string,len(e))
	for i,ee := range e { s[i] = ee.String() }
	return "anyof("+strings.Join(s,", ")+")"
}
func(e Any) Type() sql.Type {
	if len(e)==0 { return sql.Null }
	return e[0].Type()
}
func(e Any) IsNullable() bool {
	if len(e)==0 { return true }
	return e[0].IsNullable()
}
func(e Any) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if len(e)==0 { return nil,nil }
	return e[0].Eval(ctx,row)
}
func(e Any) TransformUp(tf sql.TransformExprFunc) (_ sql.Expression, err error) {
	ne := make(Any,len(e))
	for i,ee := range e {
		ne[i],err = tf(ee)
		if err!=nil { return }
	}
	return tf(ne)
}
func(e Any) Children() []sql.Expression { return e }
func(e Any) isInvalid() {}

func NewAny(exprs ...sql.Expression) (sql.Expression, error) { return Any(exprs),nil }

func getStrangeList(expr sql.Expression) ([]sql.Expression,uint) {
	switch v := expr.(type) {
	case Any: return v,1
	case Each: return v,2
	}
	return nil,0
}

func joinOr(exprs []sql.Expression) sql.Expression {
	if len(exprs)==0 { return nil }
	ex := exprs[0]
	for _,o := range exprs[1:] {
		ex = expression.NewOr(ex,o)
	}
	return ex
}

/*
x < y == lowComp(x) < highComp(y)
*/
func lowComp(expr sql.Expression) sql.Expression {
	switch v := expr.(type) {
	case Any: return Lowest(v)
	case Each: return Highest(v)
	}
	return expr
}
func highComp(expr sql.Expression) sql.Expression {
	switch v := expr.(type) {
	case Any: return Highest(v)
	case Each: return Lowest(v)
	}
	return expr
}

func convertSpecialOne(expr sql.Expression) (sql.Expression, error) {
	switch v := expr.(type) {
	case Each: if len(v)==1 { return v[0],nil }
	case Any: if len(v)==1 { return v[0],nil }
	case *expression.Equals:
		ll,tl := getStrangeList(v.Left())
		lr,tr := getStrangeList(v.Right())
		switch (tl<<4)|tr {
		case 0x22: /* each(...) = each(...) */
			eq := make(Equal,0,len(ll)+len(lr))
			eq = append(append(eq,ll...),lr...)
			return eq,nil
		case 0x02: /* scalar = each(...) */
			eq := make(Equal,1,len(lr)+1)
			eq[0] = v.Left()
			eq = append(eq,lr...)
			return eq,nil
		case 0x20: /* each(...) = scalar */
			eq := make(Equal,1,len(ll)+1)
			eq[0] = v.Right()
			eq = append(eq,ll...)
			return eq,nil
		case 0x10: return expression.NewIn(v.Right(),expression.Tuple(ll)),nil
		case 0x01: return expression.NewIn(v.Left(),expression.Tuple(lr)),nil
		case 0x11:
			or := make([]sql.Expression,len(ll))
			for i,subex := range ll {
				s,e := convertSpecialOne(expression.NewEquals(subex,v.Right()))
				if e!=nil { return nil,e }
				or[i] = s
			}
			if len(or)==0 { return nil,fmt.Errorf("invlid %q = ...",v.Left()) }
			return joinOr(or),nil
		case 0x12:
			and := make([]sql.Expression,1,1+len(lr))
			and[0] = Equal(lr)
			for _,subex := range lr {
				and = append(and,expression.NewIn(subex,expression.Tuple(ll)))
			}
			return expression.JoinAnd(and...),nil
		case 0x21:
			and := make([]sql.Expression,1,1+len(ll))
			and[0] = Equal(ll)
			for _,subex := range ll {
				and = append(and,expression.NewIn(subex,expression.Tuple(lr)))
			}
			return expression.JoinAnd(and...),nil
		}
	case *expression.In:
		ll,tl := getStrangeList(v.Left())
		tup,_ := v.Right().(expression.Tuple)
		switch tl {
		case 1:
			or := make([]sql.Expression,len(ll))
			for i,subex := range ll {
				or[i] = expression.NewIn(subex,v.Right())
			}
			if len(or)==0 { return nil,fmt.Errorf("invlid %q = ...",v.Left()) }
			return joinOr(or),nil
		case 2:
			and := make([]sql.Expression,1,1+len(ll))
			if len(tup)==1 {
				and[0] = tup[0]
				and = append(and,ll...)
				return Equal(and),nil
			}
			and[0] = Equal(ll)
			for _,subex := range ll {
				and = append(and,expression.NewIn(subex,v.Right()))
			}
			return expression.JoinAnd(and...),nil
		}
	case *expression.LessThan: return expression.NewLessThan(lowComp(v.Left()),highComp(v.Right())),nil
	case *expression.LessThanOrEqual: return expression.NewLessThanOrEqual(lowComp(v.Left()),highComp(v.Right())),nil
	case *expression.GreaterThan: return expression.NewGreaterThan(highComp(v.Left()),lowComp(v.Right())),nil
	case *expression.GreaterThanOrEqual: return expression.NewGreaterThanOrEqual(highComp(v.Left()),lowComp(v.Right())),nil
	}
	return expr,nil
}


