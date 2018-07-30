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


type Bitmap []uint64
func (bmp Bitmap) Set(pos uint64) {
	bmp[pos>>6] |= uint64(1)<<(pos&63)
}
func (bmp Bitmap) Get(pos uint64) uint64 {
	return (bmp[pos>>6]>>(pos&63))&1
}
func (bmp *Bitmap) GrowAndClear(nElems uint64) {
	nElems +=63
	nElems >>= 6
	if uint64(cap(*bmp))<nElems {
		*bmp = make(Bitmap,nElems)
	} else {
		*bmp = (*bmp)[:nElems]
		nb := *bmp
		for i := range nb { nb[i]=0 }
	}
}

