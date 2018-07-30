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


package mathpp

import "math"

func CalculateNK(n uint,fpRate float64) (N,K uint){
	const logFill = 0.4804530139182014 /* log(fillRatio)*log(1-fillRatio) with fillRatio = 0.5  */
	N = uint(math.Ceil(float64(n) * math.Abs(math.Log(fpRate)) / logFill))
	K = uint(math.Ceil(math.Abs(math.Log2(fpRate))))
	return
}

func check(n, num uint) (k uint) {
	t := uint(1)
	for t<num {
		t *= n
		k++
		n--
	}
	return
}
