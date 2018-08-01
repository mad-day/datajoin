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

import "github.com/mad-day/datajoin/join/hashjoin"
import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "io"
import "context"


func (r *RealJoin) getPreferedX(x int) int {
	if r.Chunk==0 { return x }
	return r.Chunk
}
func (r *RealJoin) getPreferedBufferSize() int { return r.getPreferedX(128) }
func (r *RealJoin) getPreferedChunkSize_One() int { return r.getPreferedX(1024) }
func (r *RealJoin) getPreferedChunkSize_Two() int { return r.getPreferedX(128) }


func (r *RealJoin) Resolved() bool { return true }
func (r *RealJoin) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) { return f(r) }
func (r *RealJoin) TransformExpressionsUp(sql.TransformExprFunc) (sql.Node, error) { return r,nil }
func (r *RealJoin) Schema() (s sql.Schema) {
	for _,child := range r.Tables {
		s = append(s,child.Schema()...)
	}
	return s
}
func (r *RealJoin) Children() []sql.Node { return nil }
func (r *RealJoin) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var cancel func()
	nctx := new(sql.Context)
	*nctx = *ctx
	nctx.Context,cancel = context.WithCancel(nctx.Context)
	ri := &rowIter{nctx,make(chan sql.Row,r.getPreferedBufferSize()),cancel}
	
	pi := &hashjoin.PassingIterator{Ctx:nctx,Endpt:ri,Hashes:r.MergeHashes(),Postfilters:r.Postfilter,Chunk:r.getPreferedChunkSize_One()}
	go func() {
		defer close(ri.buffer)
		r.IterateOver(nctx,pi,r.getPreferedChunkSize_Two())
	}()
	
	return ri,nil
	//return nil,fmt.Errorf("not implemented!")
}
var _ sql.Node = (*RealJoin)(nil)


type rowIter struct{
	ctx *sql.Context
	buffer chan sql.Row
	cancel func()
}
func (ri *rowIter) PassResults(rs []sql.Row) error {
	done := ri.ctx.Done()
	for _,row := range rs {
		select {
		case ri.buffer <- row:
		case <- done:
			return ri.ctx.Err()
		}
	}
	return nil
}
func (ri *rowIter) Close() error {
	if ri.cancel!=nil {
		ri.cancel()
		ri.cancel = nil
	}
	return nil
}
func (ri *rowIter) Next() (sql.Row, error) {
	select {
	case row := <- ri.buffer:
		if len(row)==0 { return nil,io.EOF }
		return row,nil
	case <- ri.ctx.Done():
		return nil,ri.ctx.Err()
	}
	panic("unreachable")
}
var _ sql.RowIter = (*rowIter)(nil)

