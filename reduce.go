package mapreduce

import "context"

type ReducerFunc[T any] func(context.Context, T, T) (T, OperationType, error)

func Reduce[T any](ctx context.Context, identity T, inputs []T, accumulator ReducerFunc[T]) (T, OperationType, error) {
	result := identity
	opCode := Continue
	var err error
	for idx := range inputs {
		result, opCode, err = accumulator(ctx, result, inputs[idx])
		if Continue != opCode {
			break
		}
	}

	return result, opCode, err
}
