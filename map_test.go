package mapreduce

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func convertFunc(ctx context.Context, id string, opTypeWhenError OperationType) (int, OperationType, error) {
	opType := Continue

	//sleep random time to simulate slow business processing (such as remote downloads, etc.)
	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)

	num, err := strconv.Atoi(id)
	if err != nil {
		opType = opTypeWhenError
	}
	return num, opType, err
}

func convertStopFunc(ctx context.Context, id string) (int, OperationType, error) {
	return convertFunc(ctx, id, Stop)
}

func convertContinueFunc(ctx context.Context, id string) (int, OperationType, error) {
	return convertFunc(ctx, id, Continue)
}

func numberErrHelper(Num string) error {
	return &strconv.NumError{Func: "Atoi", Num: Num, Err: strconv.ErrSyntax}
}

func TestMap(t *testing.T) {
	type args struct {
		ctx         context.Context
		inputs      []string
		concurrency int
		mapper      MapperFunc[string, int]
	}
	tests := []struct {
		name     string
		args     args
		want     []int
		wantErrs []error
		opType   OperationType
	}{
		{
			name: "all successful",
			args: args{ctx: context.Background(),
				inputs: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, concurrency: runtime.NumCPU(), mapper: convertStopFunc,
			},
			want:     []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			wantErrs: []error{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
			opType:   Continue,
		},

		{
			name:     "error with continue",
			args:     args{ctx: context.Background(), inputs: []string{"1", "not", "3"}, concurrency: 1, mapper: convertContinueFunc},
			want:     []int{1, 0, 3},
			wantErrs: []error{nil, numberErrHelper("not"), nil},
			opType:   Continue,
		},

		{
			name: "error with stop",
			args: args{ctx: context.Background(), inputs: []string{"1", "2", "3", "not_exist", "4", "5", "6"},
				concurrency: 1, mapper: convertStopFunc},
			want:     []int{1, 2, 3, 0},
			wantErrs: []error{nil, nil, nil, numberErrHelper("not_exist")},
			opType:   Stop,
		},

		{
			name: "error at last",
			args: args{ctx: context.Background(), inputs: []string{"1", "2", "3", "not_exist"},
				concurrency: 1, mapper: convertStopFunc},
			want:     []int{1, 2, 3, 0},
			wantErrs: []error{nil, nil, nil, numberErrHelper("not_exist")},
			opType:   Stop,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if true {
				got := Map(tt.args.ctx, tt.args.inputs, tt.args.mapper)
				//slog.Info(fmt.Sprintf("Map name=%s, got=%+v", tt.name, got))
				realResult, errs, opType := got.ConvertResult()
				if !reflect.DeepEqual(tt.want, realResult) ||
					!reflect.DeepEqual(tt.wantErrs, errs) ||
					tt.opType != opType {
					t.Errorf("not eqaul, want:%v, real:%v, wantErrs:%v, err:%v, wantOpType:%s, opType:%s",
						tt.want, realResult,
						tt.wantErrs, errs,
						tt.opType, opType)
				}
			}
			if true {
				got := ParallelMap(tt.args.ctx, tt.args.inputs, tt.args.concurrency, tt.name, tt.args.mapper)
				//slog.Info(fmt.Sprintf("ParallelMap name=%s, got=%+v", tt.name, got))
				realResult, errs, opType := got.ConvertResult()

				if !reflect.DeepEqual(tt.want, realResult) ||
					!reflect.DeepEqual(tt.wantErrs, errs) ||
					tt.opType != opType {
					t.Errorf("not eqaul, want:%v, real:%v, wantErrs:%v, err:%v, wantOpType:%s, opType:%s",
						tt.want, realResult,
						tt.wantErrs, errs,
						tt.opType, opType)
				}
			}
		})
	}
}

func TestStreamMap(t *testing.T) {
	ctx := context.Background()
	inItemCount := 50
	chInput := make(chan string)
	go func() {
		for i := 0; i < inItemCount; i++ {
			idx := rand.Intn(100)
			chInput <- fmt.Sprintf("%d", idx)
		}
		close(chInput)
	}()
	//start 10 goroutines to handle 50 item
	chOutput := StreamMap(ctx, 10, "testStreamMap", 50, chInput, convertContinueFunc)

	mapResultCount := 0
	for outItem := range chOutput {
		_ = outItem
		mapResultCount++
		//t.Logf("outItem=%v", outItem)
	}
	if inItemCount != mapResultCount {
		t.Errorf("inItemCount == mapResultCount")
	}
}
