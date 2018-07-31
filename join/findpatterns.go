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

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "github.com/mad-day/datajoin/join/matcher"
import "github.com/mad-day/datajoin/join/hashjoin"

type HashReducer struct{
	Tables []int
	Hashes [][]sql.Expression
}

func (r *RealJoin) getBitMaps() []uint64 {
	var u,shard uint64
	bm := make([]uint64,len(r.Tables))
	for i,tab := range r.Tables {
		var ts matcher.TableSet = matcher.TableSetSimple(tab.Name())
		u = 1
		shard = 0
		for _,expr := range r.Equals {
			if matcher.IsSatisfiedEqual(expr,ts) {
				shard |= u
			}
			u<<=1
			if u==0 { break }
		}
		bm[i] = shard
	}
	return bm
}
func (r *RealJoin) FindPatterns() {
	r.getBitMaps()
}
func (r *RealJoin) GetReducers() (lst []*HashReducer) {
	bm := r.getBitMaps()
	cnt := make([]uint64,len(bm))
	shard := ^(uint64(0))
	l := 0
	for i,b := range bm {
		if (b&shard)==0 { break }
		shard &= b
		l = i+1
	}
	if l>1 {
		hr := new(HashReducer)
		for i := 0 ; i<l ; i++ {
			hr.Tables = append(hr.Tables,i)
			cnt[i]++
		}
		lst = append(lst,hr)
	}
	
	for l<len(bm) {
		hr := new(HashReducer)
		shard = bm[l]
		for i := 0 ; i<l ; i++ {
			b := bm[i]
			if (b&shard)==0 { break }
			shard &= b
			hr.Tables = append(hr.Tables,i)
			cnt[i]++
		}
		hr.Tables = append(hr.Tables,l)
		if len(hr.Tables)<2 { break }
		cnt[l]++
		l++
		lst = append(lst,hr)
	}
	
	for _,hr := range lst { r.GetHashForReducer(hr) }
	return
}
func (r *RealJoin) GetHashFor(tss ...matcher.TableSet) (perTS [][]sql.Expression) {
	var perX [][]sql.Expression
	grand: for _,expr := range r.Equals {
		buf := make([]sql.Expression,len(tss))
		for i,ts := range tss {
			buf[i] = matcher.GetValueForHash(expr,ts)
			if buf[i]==nil { continue grand }
		}
		perX = append(perX,buf)
	}
	l := len(perX)
	perTS = make([][]sql.Expression,len(tss))
	for j := range tss {
		perTS[j] = make([]sql.Expression,l)
	}
	for i,vec := range perX {
		for j,expr := range vec {
			perTS[j][i] = expr
		}
	}
	return perTS
}
func (r *RealJoin) GetHashForReducer(hr *HashReducer) {
	tss := make([]matcher.TableSet,len(hr.Tables))
	for i,j := range hr.Tables { tss[i] = matcher.TableSetSimple(r.Tables[j].Name()) }
	hr.Hashes = r.GetHashFor(tss...)
	for i,lst := range hr.Hashes {
		for j := range lst {
			lst[j],_ = lst[j].TransformUp(matcher.Indent(-r.Offsets[i]))
		}
	}
}

func (r *RealJoin) MergeHashes() (sm hashjoin.MergeHashes){
	sm = make(hashjoin.MergeHashes,len(r.Tables))
	for i,l := 1,len(r.Tables) ; i < l ; i++ {
		hf := r.GetHashFor(matcher.TableSetFor(r.Tables[:i]),matcher.TableSetFor(r.Tables[i:][:1]))
		sm[i].Left  = hf[0]
		sm[i].Right = hf[1]
		for j,ex := range sm[i].Right { sm[i].Right[j],_ = ex.TransformUp(matcher.Indent(-r.Offsets[i])) }
	}
	return
}


