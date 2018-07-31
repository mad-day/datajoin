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
import "github.com/mad-day/datajoin/join/apis"
import "golang.org/x/crypto/blake2b"
import farm "github.com/dgryski/go-farm"
import "hash"

type MergeTableHash struct{
	Left, Right []sql.Expression
}
type MergeHashes []MergeTableHash

type PassingIterator struct{
	Ctx    *sql.Context
	Endpt  apis.ResultEndpoint
	Hashes MergeHashes
	Tables []*TrueHashTable
	Chunk  int
	fu  hash.Hash
	buf []byte
	result []sql.Row
}
func (pi *PassingIterator) PassTabBlockRow(tabs [][]sql.Row) error {
	if pi.fu==nil {
		pi.fu,_ = blake2b.New512(nil)
	}
	if len(pi.Tables)<len(tabs) {
		pi.Tables = make([]*TrueHashTable,len(tabs))
		for i := range pi.Tables {
			if i==0 { continue }
			pi.Tables[i] = new(TrueHashTable)
		}
	}
	
	width := 0
	for _,block := range tabs {
		/*
		If one of the operands is empty, the cartesian product will be empty as well.
		*/
		if len(block)==0 { return nil }
		width += len(block[0])
	}
	
	/* Hash the Table blocks. */
	for j,block := range tabs[1:] {
		i := j+1
		err := pi.Tables[i].SetRows(block,func(row sql.Row) (h1,h2 uint64,e error) {
			pi.buf,e = Hash(pi.buf,pi.Ctx,row,pi.fu,pi.Hashes[i].Right...)
			h1,h2 = farm.Hash128(pi.buf)
			return
		})
		if err!=nil { return err }
	}
	
	if len(pi.result)!=0 {
		pi.result = pi.result[:0]
	}
	
	wblk := make(sql.Row,0,width)
	for _,row := range tabs[0] {
		err := pi.perform(1,append(wblk,row...))
		if err!=nil { return err }
	}
	
	/* Flush the rest of the resultset. */
	if len(pi.result)!=0 {
		return pi.Endpt.PassResults(pi.result)
	}
	
	return nil
}
func (pi *PassingIterator) perform(i int,row sql.Row) (e error) {
	if len(pi.Tables)<=i {
		/* Make a copy of this row. */
		coro := make(sql.Row,len(row))
		copy(coro,row)
		
		/* Append this copy to the result-set. */
		pi.result = append(pi.result,coro)
		
		/* If the resultset reaches a certain size, flush it. */
		if len(pi.result)>=pi.Chunk {
			rs := pi.result
			pi.result = pi.result[:0]
			return pi.Endpt.PassResults(rs)
		}
		return nil
	}
	pi.buf,e = Hash(pi.buf,pi.Ctx,row,pi.fu,pi.Hashes[i].Left...)
	h1,h2 := farm.Hash128(pi.buf)
	if e!=nil { return }
	
	ret := pi.Tables[i].LookupDirect([2]uint64{h1,h2})
	for _,right := range ret {
		nr := append(row,right...)
		e = pi.perform(i+1,nr)
		if e!=nil { return }
	}
	return nil
}

