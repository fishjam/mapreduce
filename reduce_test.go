package mapreduce

import (
	"context"
	"testing"
)

func TestReduceSumInt(t *testing.T) {
	ctx := context.Background()
	var opCode OperationType
	var err error

	var resultSumInt int
	resultSumInt, opCode, err = Reduce(ctx, 0, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		func(ctx context.Context, t1 int, t2 int) (int, OperationType, error) {
			return t1 + t2, Continue, nil
		})

	if err != nil || opCode != Continue {
		t.Error("Reduce should successful")
	}
	if resultSumInt != 55 {
		t.Errorf("resultSumInt == %d, want %d", resultSumInt, 55)
	}
}

func TestReduceConcatStr(t *testing.T) {
	ctx := context.Background()
	var opCode OperationType
	var err error

	var resultConcatStr string
	resultConcatStr, opCode, err = Reduce(ctx, "", []string{"hello", "go", "study"},
		func(ctx context.Context, t1 string, t2 string) (string, OperationType, error) {
			if len(t1) > 0 {
				return t1 + " " + t2, Continue, nil
			}
			return t2, Continue, nil // first ""
		})
	if err != nil || opCode != Continue {
		t.Error("Reduce should successful")
	}
	if resultConcatStr != "hello go study" {
		t.Errorf("resultSumInt == %s", resultConcatStr)
	}
}
