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


package hashjoin

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "fmt"
import "hash"
import "sort"
//import "github.com/dgryski/go-farm"
//import "golang.org/x/crypto/blake2b"
//import boom "github.com/tylertreat/BoomFilters"



func Hash(buf []byte,ctx *sql.Context,row sql.Row,f hash.Hash,exprs ...sql.Expression) ([]byte,error) {
	f.Reset()
	for _,expr := range exprs {
		v,err := expr.Eval(ctx,row)
		if err!=nil { return nil,err }
		fmt.Fprint(f,v)
	}
	return f.Sum(buf[:0]),nil
}

func isLessHash(a,b [2]uint64) bool {
	if a[0]>b[0] { return false }
	if a[0]<b[0] { return true }
	return a[1]<b[1]
}

type TrueHashTable struct{
	Map    map[[2]uint64][2]uint
	Hashes [][2]uint64
	Rows   []sql.Row
}
type xTht TrueHashTable
func (tht *xTht) Len() int { return len(tht.Hashes) }
func (tht *xTht) Less(i, j int) bool { return isLessHash(tht.Hashes[i],tht.Hashes[j]) }
func (tht *xTht) Swap(i, j int) {
	tht.Hashes[i],tht.Hashes[j] = tht.Hashes[j],tht.Hashes[i]
	tht.Rows[i],tht.Rows[j] = tht.Rows[j],tht.Rows[i]
}
func (tht *TrueHashTable) SetRows(rows []sql.Row,hf func(row sql.Row) (h1,h2 uint64,e error)) error {
	if len(rows)==0 {
		*tht = TrueHashTable{Hashes:tht.Hashes[:0]}
		return nil
	}
	if cap(tht.Rows)<len(rows) {
		tht.Rows = make([]sql.Row,len(rows))
	} else {
		tht.Rows = tht.Rows[:len(rows)]
	}
	copy(tht.Rows,rows)
	if cap(tht.Hashes)<len(rows) {
		tht.Hashes = make([][2]uint64,len(rows))
	} else {
		tht.Hashes = tht.Hashes[:len(rows)]
	}
	for i,row := range rows {
		h1,h2,err := hf(row)
		if err!=nil { return err }
		tht.Hashes[i] = [2]uint64{h1,h2}
	}
	sort.Sort((*xTht)(tht))
	tht.Map = make(map[[2]uint64][2]uint)
	last := tht.Hashes[0]
	start := uint(0)
	for i,hsh := range tht.Hashes {
		if hsh==last { continue }
		tht.Map[last] = [2]uint{start,uint(i)}
		last = hsh
		start = uint(i)
	}
	tht.Map[last] = [2]uint{start,uint(len(tht.Hashes))}
	return nil
}
func (tht *TrueHashTable) LookupDirect(h [2]uint64) []sql.Row {
	pair := tht.Map[h]
	return tht.Rows[pair[0]:pair[1]]
}


