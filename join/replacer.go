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


package join

import "github.com/mad-day/datajoin/query"
import "gopkg.in/src-d/go-mysql-server.v0/sql"

func MakeExecutable(node sql.Node) (sql.Node,error) {
	repl := make(map[*query.Cookie]sql.Node)
	for _,mj := range GetAll(node) {
		repl[mj.Cookie] = NewRealJoin(mj)
	}
	return node.TransformUp(func(node sql.Node) (sql.Node,error){
		switch v := node.(type) {
		case *query.MultiJoin:
			r,ok := repl[v.Cookie]
			if ok { return r,nil }
		}
		return node,nil
	})
}