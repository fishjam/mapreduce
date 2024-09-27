package mapreduce

import "fmt"

type OperationType uint8

const (
	Continue OperationType = iota // 继续处理后续 Item, 即使有错也忽略
	Stop                          // 立即停止处理 -- 比如文件拷贝任务,但当目标磁盘满时需要立刻停止
)

func (op OperationType) String() string {
	switch op {
	case Continue:
		return "continue"
	case Stop:
		return "stop"
	default:
		return fmt.Sprintf("unknown:%d", op)
	}
}

type InputItem[T any] struct {
	index int64
	item  T
}

type OutputItem[T any, R any] struct {
	Index  int64
	Item   T
	OpType OperationType
	Result R
	Err    error
}

func (oi *OutputItem[T, R]) String() string {
	str := fmt.Sprintf("index:%d, err:%v, opType:%s, item:%v, result:%v",
		oi.Index, oi.Err, oi.OpType, oi.Item, oi.Result)
	return str
}
