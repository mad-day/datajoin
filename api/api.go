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


package api

import "io"
import "reflect"

type Row []interface{}
type RowIter interface{
	io.Closer
	Next() bool
	Fetch() (Row,error)
}
type RowSource interface{
	Names() []string
	Types() []reflect.Type
	Lookup(i ... interface{}) (RowIter,error)
}

type DataSource interface{
	GetSource(name string) RowSource
}

/*
Array-spec.
${Column} = any(${Values}).
*/
type Spec struct{
	Column string
	Values interface{}
}

type SpecSingle struct{
	Column string
	Value  interface{}
}

type DataSourceImpl map[string]RowSource
func (dsi DataSourceImpl) GetSource(name string) RowSource { return dsi[name] }



