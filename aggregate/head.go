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

var Defaults = sql.Functions{
	// Group-By-Hints.
	"row_cluster":sql.FunctionN(NewRowCluster),
	
	// Aggregations
	"as_list":sql.FunctionN(NewAsList),
	"first"  :sql.Function1(NewFirst),
	"last"   :sql.Function1(NewLast),
	
	// Compound aggregations
	"filter"  :sql.FunctionN(NewFilter),
	"group_by":sql.FunctionN(NewGroupBy),
	
	// Regular functions
	"dict" :sql.FunctionN(NewDict),
	"array":sql.FunctionN(NewArray),
}

