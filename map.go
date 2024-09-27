package mapreduce

import (
	"context"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// MapperFunc map input T to output R
// In the process of mapping, whether to continue processing subsequent items is determined by OperationType, not by error
type MapperFunc[T any, R any] func(context.Context, T) (R, OperationType, error)

// ResultsMap will save all input item's index map to result Item(OutputItem)
// The reason is: ParallelMap and StreamMap might return the results not same as input order.
// and their might exist error.
type ResultsMap[T any, R any] map[int64]*OutputItem[T, R]

// ConvertResult Convert the map to ordered slice as input order
func (r *ResultsMap[T, R]) ConvertResult() ([]R, []error, OperationType) {
	opType := Continue
	results := make([]R, 0, len(*r))
	errs := make([]error, 0, len(*r))

	ids := maps.Keys(*r)
	slices.Sort(ids)

	for idx := range ids {
		results = append(results, (*r)[int64(idx)].Result)
		errs = append(errs, (*r)[int64(idx)].Err)

		if (*r)[int64(idx)].OpType != Continue {
			opType = (*r)[int64(idx)].OpType
		}
	}
	return results, errs, opType
}

// Map Serial map function
func Map[T any, R any](ctx context.Context, inputs []T, mapper MapperFunc[T, R]) ResultsMap[T, R] {
	results := make(ResultsMap[T, R])
	for idx, item := range inputs {
		r, opType, err := mapper(ctx, item)
		results[int64(idx)] = &OutputItem[T, R]{Index: int64(idx), OpType: opType, Item: item, Result: r, Err: err}
		if Continue != opType {
			break
		}
	}
	return results
}

type ParallelMapImpl[T any, R any] struct {
	ctx         context.Context
	concurrency int
	name        string
	wg          sync.WaitGroup
	chInput     chan *InputItem[T]
	chOutput    chan *OutputItem[T, R]
	willStop    atomic.Bool //if mapper func return Stop, then will make all goroutines in this map stop
}

func NewParallelMapImpl[T any, R any](ctx context.Context, concurrency int, name string) *ParallelMapImpl[T, R] {
	parallelMap := &ParallelMapImpl[T, R]{
		ctx:         ctx,
		name:        name,
		concurrency: concurrency,
		chInput:     make(chan *InputItem[T], concurrency),
		chOutput:    make(chan *OutputItem[T, R], concurrency),
	}
	parallelMap.willStop.Store(false)
	return parallelMap
}

func (m *ParallelMapImpl[T, R]) Close() error {
	//ask all goroutines in this class stop
	m.willStop.Store(true)
	return nil
}

// workerFunc work goroutine, read input item from input chan, after handled by mapper, write to output chan.
func (m *ParallelMapImpl[T, R]) workerFunc(idx int, mapper MapperFunc[T, R]) {
	defer m.wg.Done()
	for inputItem := range m.chInput {

		if m.willStop.Load() {
			break
		}

		r, opType, err := mapper(m.ctx, inputItem.item)
		m.chOutput <- &OutputItem[T, R]{
			Index:  inputItem.index,
			Item:   inputItem.item,
			OpType: opType,
			Result: r,
			Err:    err,
		}

		if Continue != opType {
			m.willStop.Store(true)
			break
		}
	}
}

func (m *ParallelMapImpl[T, R]) startWorkerAndMonitor(realConcurrency int, mapper MapperFunc[T, R]) {

	for i := 0; i < realConcurrency; i++ {
		m.wg.Add(1)
		go func(idx int) {
			m.workerFunc(idx, mapper)
		}(i)
	}

	go func() {
		// wait all sub goroutines quit, and then close output chan
		m.wg.Wait()
		close(m.chOutput)
	}()
}

func (m *ParallelMapImpl[T, R]) Start(inputs []T, mapper MapperFunc[T, R]) chan *OutputItem[T, R] {
	realConcurrency := m.concurrency
	if len(inputs) < realConcurrency {
		realConcurrency = len(inputs)
	}
	m.startWorkerAndMonitor(realConcurrency, mapper)

	// start producer
	go func() {
		defer close(m.chInput)
		for idx := range inputs {
			if m.willStop.Load() {
				break
			}
			m.chInput <- &InputItem[T]{
				index: int64(idx),
				item:  inputs[idx],
			}
		}
	}()

	return m.chOutput
}

func (m *ParallelMapImpl[T, R]) StartWithChan(startIndex int64, chData chan T, mapper MapperFunc[T, R]) chan *OutputItem[T, R] {
	m.startWorkerAndMonitor(m.concurrency, mapper)

	// start producer
	go func() {
		defer close(m.chInput)
		idx := startIndex
		for inData := range chData {
			if m.willStop.Load() {
				break
			}
			idx++
			m.chInput <- &InputItem[T]{
				index: idx,
				item:  inData,
			}
		}
	}()

	return m.chOutput
}

func (m *ParallelMapImpl[T, R]) ReadMapperResult(chOutput chan *OutputItem[T, R]) ResultsMap[T, R] {
	results := make(map[int64]*OutputItem[T, R])
	receiveOrder := 1
	for outputs := range chOutput {
		results[outputs.Index] = outputs
		receiveOrder++
	}
	return results
}

// ParallelMap  Concurrent map, will start 2 + concurrency goroutines to handle all items in inputs
func ParallelMap[T any, R any](ctx context.Context, inputs []T, concurrency int, name string, mapper MapperFunc[T, R]) ResultsMap[T, R] {
	parallelMap := NewParallelMapImpl[T, R](ctx, concurrency, name)
	chOutput := parallelMap.Start(inputs, mapper)
	return parallelMap.ReadMapperResult(chOutput)
}

// StreamMap Infinite map, Similar to a goroutines pool, read data from chInput, after handled by mapper,
// write the result to out chan.
func StreamMap[T any, R any](ctx context.Context, concurrency int, name string,
	startIndex int64, chInput chan T, mapper MapperFunc[T, R]) chan *OutputItem[T, R] {
	parallelMap := NewParallelMapImpl[T, R](ctx, concurrency, name)
	chOutput := parallelMap.StartWithChan(startIndex, chInput, mapper)
	return chOutput
}
