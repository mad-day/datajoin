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
import "fmt"

type x_cluster []sql.Expression
var _ sql.Expression = x_cluster(nil)
//Implements sql.Expression
func (x x_cluster) Resolved() (ok bool) {
	ok = true
	for _,y := range x { ok = ok || y.Resolved() }
	return
}
//Implements sql.Expression
func (x x_cluster) String() string {
	return expression.Tuple(x).String()
}
//Implements sql.Expression
func (x_cluster) Type() sql.Type { return sql.Uint64 }
//Implements sql.Expression
func (x_cluster) IsNullable() bool { return false }
//Implements sql.Expression
func (x_cluster) Eval(*sql.Context, sql.Row) (interface{}, error) { return uint64(1),nil }
func (x x_cluster) transform(st sql.TransformExprFunc) (y x_cluster, e error) {
	y = make(x_cluster,len(x))
	for i,v := range x {
		y[i],e = v.TransformUp(st)
		if e!=nil { return }
	}
	return
}
//This function is not overridden. It will fail with an error!
func (x x_cluster) TransformUp(st sql.TransformExprFunc) (sql.Expression, error) { return nil,fmt.Errorf("Oops. aggregate.x_cluster is an abstract type.") }
//Implements sql.Expression
func (x x_cluster) Children() []sql.Expression { return []sql.Expression(x) }

type RowCluster struct{
	x_cluster
}
func NewRowCluster(exprs ...sql.Expression) (sql.Expression, error) {
	exprs = Clone(exprs)
	return RowCluster{exprs},nil
}
//Implements sql.Expression
func (r RowCluster) String() string {
	return "row_cluster"+r.x_cluster.String()
}
//Implements sql.Expression
func (r RowCluster) TransformUp(st sql.TransformExprFunc) (sql.Expression, error) {
	x,e := r.transform(st)
	if e!=nil { return nil,e }
	return RowCluster{x},nil
}

