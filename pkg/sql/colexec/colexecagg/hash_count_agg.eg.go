// Code generated by execgen; DO NOT EDIT.
// Copyright 2018 The Cockroach Authors.
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

func newCountRowsHashAggAlloc(
	allocator *colmem.Allocator, allocSize int64,
) aggregateFuncAlloc {
	return &countRowsHashAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

// countRowsHashAgg supports either COUNT(*) or COUNT(col) aggregate.
type countRowsHashAgg struct {
	unorderedAggregateFuncBase
	col    []int64
	curAgg int64
}

var _ AggregateFunc = &countRowsHashAgg{}

func (a *countRowsHashAgg) SetOutput(vec coldata.Vec) {
	a.unorderedAggregateFuncBase.SetOutput(vec)
	a.col = vec.Int64()
}

func (a *countRowsHashAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	var oldCurAggSize uintptr
	a.allocator.PerformOperation([]coldata.Vec{a.vec}, func() {
		{
			{
				// We don't need to pay attention to nulls (either because it's a
				// COUNT_ROWS aggregate or because there are no nulls), and we're
				// performing a hash aggregation (meaning there is a single group),
				// so all endIdx-startIdx tuples contribute to the count.
				a.curAgg += int64(endIdx - startIdx)
			}
		}
	},
	)
	var newCurAggSize uintptr
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *countRowsHashAgg) Flush(outputIdx int) {
	a.col[outputIdx] = a.curAgg
}

func (a *countRowsHashAgg) Reset() {
	a.curAgg = 0
}

type countRowsHashAggAlloc struct {
	aggAllocBase
	aggFuncs []countRowsHashAgg
}

var _ aggregateFuncAlloc = &countRowsHashAggAlloc{}

const sizeOfCountRowsHashAgg = int64(unsafe.Sizeof(countRowsHashAgg{}))
const countRowsHashAggSliceOverhead = int64(unsafe.Sizeof([]countRowsHashAgg{}))

func (a *countRowsHashAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(countRowsHashAggSliceOverhead + sizeOfCountRowsHashAgg*a.allocSize)
		a.aggFuncs = make([]countRowsHashAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

func newCountHashAggAlloc(
	allocator *colmem.Allocator, allocSize int64,
) aggregateFuncAlloc {
	return &countHashAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

// countHashAgg supports either COUNT(*) or COUNT(col) aggregate.
type countHashAgg struct {
	unorderedAggregateFuncBase
	col    []int64
	curAgg int64
}

var _ AggregateFunc = &countHashAgg{}

func (a *countHashAgg) SetOutput(vec coldata.Vec) {
	a.unorderedAggregateFuncBase.SetOutput(vec)
	a.col = vec.Int64()
}

func (a *countHashAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, startIdx, endIdx int, sel []int,
) {
	var oldCurAggSize uintptr
	// If this is a COUNT(col) aggregator and there are nulls in this batch,
	// we must check each value for nullity. Note that it is only legal to do a
	// COUNT aggregate on a single column.
	nulls := vecs[inputIdxs[0]].Nulls()
	a.allocator.PerformOperation([]coldata.Vec{a.vec}, func() {
		{
			if nulls.MaybeHasNulls() {
				for _, i := range sel[startIdx:endIdx] {

					var y int64
					y = int64(0)
					if !nulls.NullAt(i) {
						y = 1
					}
					a.curAgg += y
				}
			} else {
				// We don't need to pay attention to nulls (either because it's a
				// COUNT_ROWS aggregate or because there are no nulls), and we're
				// performing a hash aggregation (meaning there is a single group),
				// so all endIdx-startIdx tuples contribute to the count.
				a.curAgg += int64(endIdx - startIdx)
			}
		}
	},
	)
	var newCurAggSize uintptr
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *countHashAgg) Flush(outputIdx int) {
	a.col[outputIdx] = a.curAgg
}

func (a *countHashAgg) Reset() {
	a.curAgg = 0
}

type countHashAggAlloc struct {
	aggAllocBase
	aggFuncs []countHashAgg
}

var _ aggregateFuncAlloc = &countHashAggAlloc{}

const sizeOfCountHashAgg = int64(unsafe.Sizeof(countHashAgg{}))
const countHashAggSliceOverhead = int64(unsafe.Sizeof([]countHashAgg{}))

func (a *countHashAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(countHashAggSliceOverhead + sizeOfCountHashAgg*a.allocSize)
		a.aggFuncs = make([]countHashAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}