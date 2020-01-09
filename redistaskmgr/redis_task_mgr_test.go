package redistaskmgr

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-courier/mq/worker"

	"github.com/go-courier/mq"
	"github.com/gomodule/redigo/redis"

	. "github.com/onsi/gomega"
)

var taskMgr = NewRedisTaskMgr(r)

var channel = "TEST"

var r = RedisOperatorFromPool(&redis.Pool{
	Dial: func() (redis.Conn, error) {
		return redis.Dial(
			"tcp",
			"localhost:6379",
			redis.DialDatabase(10),
			redis.DialWriteTimeout(10*time.Second),
			redis.DialConnectTimeout(10*time.Second),
			redis.DialReadTimeout(10*time.Second),
		)
	},
	MaxIdle:     5,
	MaxActive:   3,
	IdleTimeout: 10 * time.Second,
	Wait:        true,
})

func init() {
	taskMgr.Destroy(channel)
}

func BenchmarkTaskMgr(b *testing.B) {
	for i := 0; i < b.N; i++ {
		taskMgr.Push(channel, mq.NewTask("TEST", nil, fmt.Sprintf("%d", i)))
		taskMgr.Shift(channel)
	}
}

func TestSingle(t *testing.T) {
	taskMgr.Push(channel, mq.NewTask("TEST", nil, "11"))
	task, err := taskMgr.Shift(channel)

	NewWithT(t).Expect(err).To(BeNil())
	NewWithT(t).Expect(task).NotTo(BeNil())
}

func TestTaskMgrEmptyShift(t *testing.T) {
	_, err := taskMgr.Shift(channel)
	NewWithT(t).Expect(err).To(BeNil())
}

func TestTaskMgr(t *testing.T) {
	n := 1000

	for i := 0; i < n; i++ {
		taskMgr.Push(channel, mq.NewTask("TEST", nil, fmt.Sprintf("%d", i)))
		taskMgr.Push(channel, mq.NewTask("TEST", nil, fmt.Sprintf("%d", i)))
		taskMgr.Push(channel, mq.NewTask("TEST", nil, fmt.Sprintf("%d", i)))
		taskMgr.Push(channel, mq.NewTask("TEST", nil, fmt.Sprintf("%d", i)))
		taskMgr.Push(channel, mq.NewTask("TEST", nil, fmt.Sprintf("%d", i)))
	}

	wg := sync.WaitGroup{}
	wg.Add(n)

	w := worker.NewWorker(func(ctx context.Context) error {
		task, err := taskMgr.Shift(channel)
		if err != nil {
			return err
		}
		if task == nil {
			return nil
		}
		wg.Add(-1)
		return nil
	}, n/10)

	go w.Start(context.Background())
	wg.Wait()
}
