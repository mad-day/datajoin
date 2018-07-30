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



const NBits = 11
/* Bitmap table. */
type BitmapTable struct{
	Maps   [128]Bitmap
	Hashes [][2]uint64
	Rows   []sql.Row
}
func (bt *BitmapTable) SetRows(rows []sql.Row,hf func(row sql.Row) (h1,h2 uint64,e error)) error {
	for i := range bt.Maps{
		bt.Maps[i].GrowAndClear(uint64(len(rows)))
	}
	if cap(bt.Hashes)<len(rows) {
		bt.Hashes = make([][2]uint64,len(rows))
	} else {
		bt.Hashes = bt.Hashes[:len(rows)]
	}
	bt.Rows = rows
	for i,row := range rows {
		h1,h2,err := hf(row)
		if err!=nil { return err }
		bt.Hashes[i] = [2]uint64{h1,h2}
		for j := uint64(0) ; j < NBits ; j++ {
			bt.Maps[(h1+(h2*j))&127].Set(uint64(i))
		}
	}
	return nil
}
func (bt *BitmapTable) Lookup(h [2]uint64,c chan <- sql.Row) {
	var maps [NBits]Bitmap
	for i := range maps {
		maps[i] = bt.Maps[(h[0]+(h[1]*uint64(i)))&127]
	}
	defer close(c)
	n := uint64(len(bt.Hashes))
	l := n>>6
	for i := uint64(0); i<l ; i++ {
		shard := maps[0][i]
		for m := 1; m<NBits; m++ { shard &= maps[m][i] }
		if shard==0 { continue }
		for j := uint64(0); j<64; j++ {
			ch := shard&1
			shard>>=1
			if ch==0 { continue }
			if bt.Hashes[(i<<6)|j]!=h { continue }
			c <- bt.Rows[(i<<6)|j]
		}
	}
	r := n&63
	if r!=0 {
		shard := maps[0][l]
		for m := 1; m<NBits; m++ { shard &= maps[m][l] }
		if shard==0 { return }
		for j := uint64(0); j<r; j++ {
			ch := shard&1
			shard>>=1
			if ch==0 { continue }
			if bt.Hashes[(l<<6)|j]!=h { continue }
			c <- bt.Rows[(l<<6)|j]
		}
	}
}


