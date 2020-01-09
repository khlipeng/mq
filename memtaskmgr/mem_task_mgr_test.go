package memtaskmgr

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-courier/mq/worker"

	"github.com/go-courier/mq"
)

var taskMgr = NewMemTaskMgr()

func BenchmarkTaskMgr(b *testing.B) {
	for i := 0; i < b.N; i++ {
		taskMgr.Push("TEST", mq.NewTask("", nil, fmt.Sprintf("%d", i)))
		taskMgr.Shift("TEST")
	}
}

func TestTaskMgr(t *testing.T) {
	taskMgr.Destroy("TEST")

	for i := 0; i < 1000; i++ {
		taskMgr.Push("TEST", mq.NewTask("", nil, fmt.Sprintf("%d", i)))
		taskMgr.Push("TEST", mq.NewTask("", nil, fmt.Sprintf("%d", i)))
		taskMgr.Push("TEST", mq.NewTask("", nil, fmt.Sprintf("%d", i)))
		taskMgr.Push("TEST", mq.NewTask("", nil, fmt.Sprintf("%d", i)))
		taskMgr.Push("TEST", mq.NewTask("", nil, fmt.Sprintf("%d", i)))
	}

	wg := sync.WaitGroup{}
	wg.Add(1000)

	w := worker.NewWorker(func(ctx context.Context) error {
		task, err := taskMgr.Shift("TEST")
		if err != nil {
			return err
		}
		if task == nil {
			return nil
		}
		wg.Add(-1)
		return nil
	}, 10)

	go w.Start(context.Background())
	wg.Wait()
}
