package mq_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"

	"github.com/go-courier/courier"
	"github.com/go-courier/mq/memtaskmgr"
	"github.com/go-courier/mq/redistaskmgr"
	"github.com/gomodule/redigo/redis"

	"github.com/go-courier/mq"
)

var taskMgr = memtaskmgr.NewMemTaskMgr()
var taskMgrRedis = redistaskmgr.NewRedisTaskMgr(r)

var channel = "TEST"

var r = redistaskmgr.RedisOperatorFromPool(&redis.Pool{
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", "localhost:6379", redis.DialDatabase(3))
	},
	MaxIdle:     10,
	MaxActive:   10,
	IdleTimeout: 10 * time.Second,
	Wait:        true,
})

func init() {
	taskMgr.Destroy(channel)
	taskMgrRedis.Destroy(channel)
}

type A struct {
}

func (A) Output(ctx context.Context) (interface{}, error) {
	return nil, nil
}

type B struct {
}

func (B) Output(ctx context.Context) (interface{}, error) {
	return nil, nil
}

var router = courier.NewRouter()

func TestJobQueue(t *testing.T) {
	list := []mq.TaskMgr{
		taskMgr,
		taskMgrRedis,
	}

	for i := range list {
		taskMgr := list[i]

		jobBoard := mq.NewJobBoard(taskMgr)

		n := 100

		for i := 0; i < n; i++ {
			for j := 0; j < 5; j ++ {
				jobBoard.Dispatch("TEST", mq.NewTask("A", nil, fmt.Sprintf("A%d", i)))
				jobBoard.Dispatch("TEST", mq.NewTask("B", nil, fmt.Sprintf("B%d", i)))
			}
		}

		router.Register(courier.NewRouter(&A{}))
		router.Register(courier.NewRouter(&B{}))

		wg := sync.WaitGroup{}
		wg.Add(n * 2)

		jobWorker := mq.NewJobWorker(taskMgr, mq.JobWorkerOpts{
			Channel:    "TEST",
			NumWorkers: 2,
			OnFinish: func(task *mq.Task) {
				require.Equal(t, mq.STAGE_SUCCESS, task.Stage)
				wg.Add(-1)
			},
		})

		go jobWorker.Serve(router)

		wg.Wait()
	}
}
