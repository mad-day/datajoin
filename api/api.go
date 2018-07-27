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
import "fmt"

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

type MockTable struct{
	ItsNames []string
	ItsTypes []reflect.Type
}
func NewMockTable(vrs ...interface{}) (m *MockTable) {
	m = new(MockTable)
	if (len(vrs)&1)==1 {
		vrs = vrs[:len(vrs)-1]
	}
	for i,v := range vrs {
		if (i&1)==0 {
			m.ItsNames = append(m.ItsNames,fmt.Sprint(v))
		}else{
			m.ItsTypes = append(m.ItsTypes,reflect.ValueOf(v).Elem().Type())
		}
	}
	return
}
func (m *MockTable) Names() []string {
	s := make([]string,len(m.ItsNames))
	copy(s,m.ItsNames)
	return s
}
func (m *MockTable) Types() []reflect.Type {
	s := make([]reflect.Type,len(m.ItsTypes))
	copy(s,m.ItsTypes)
	return s
}
func (m *MockTable) Lookup(i ... interface{}) (RowIter,error) { return nil,fmt.Errorf("not implemented") }
