package mq

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"

	"github.com/go-courier/courier"
	"github.com/go-courier/mq/worker"
)

func NewJobBoard(taskMgr TaskMgr) *JobBoard {
	return &JobBoard{
		taskMgr: taskMgr,
	}
}

type JobBoard struct {
	taskMgr TaskMgr
}

func (b *JobBoard) Dispatch(channel string, task *Task) error {
	if task == nil {
		return nil
	}
	return b.taskMgr.Push(channel, task)
}

type JobWorkerOpts struct {
	Channel    string
	NumWorkers int
	OnFinish   func(task *Task)
}

func NewJobWorker(taskMgr TaskMgr, opts JobWorkerOpts) *JobWorker {
	return &JobWorker{
		JobWorkerOpts: opts,
		taskMgr:       taskMgr,
	}
}

type JobWorker struct {
	JobWorkerOpts
	operators sync.Map
	taskMgr   TaskMgr
	worker    *worker.Worker
}

func (w *JobWorker) getOperatorMeta(typ string) (*courier.OperatorMeta, error) {
	op, ok := w.operators.Load(typ)
	if !ok {
		return nil, fmt.Errorf("missing operator %s", typ)
	}
	return op.(*courier.OperatorMeta), nil
}

func (w *JobWorker) Serve(router *courier.Router) error {
	w.Register(router)

	w.worker = worker.NewWorker(w.process, w.NumWorkers)
	w.worker.Start()

	return nil
}

func (w *JobWorker) Register(router *courier.Router) {
	for _, route := range router.Routes() {
		factories := route.OperatorFactories()
		if len(factories) != 1 {
			continue
		}
		f := factories[0]
		w.operators.Store(f.Type.Name(), f)
	}
}

func (w *JobWorker) Stop() {
	if w.worker != nil {
		w.worker.Stop()
	}
}

func (w *JobWorker) process() (err error) {
	task, err := w.taskMgr.Shift(w.Channel)
	if err != nil || task == nil {
		return nil
	}

	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("panic: %s; calltrace:%s", fmt.Sprint(e), string(debug.Stack()))
		}

		if err != nil {
			task.Stage = STAGE_FAILED
		} else {
			task.Stage = STAGE_SUCCESS
		}

		if w.OnFinish != nil {
			w.OnFinish(task)
		}
	}()

	opMeta, e := w.getOperatorMeta(task.Subject)
	if e != nil {
		err = e
		return
	}

	op := opMeta.New()

	if task.Argv != nil {
		if writer, ok := op.(io.Writer); ok {
			if _, e := writer.Write(task.Argv); e != nil {
				err = e
				return
			}
		}
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, "Task", task)

	if _, e := op.Output(ctx); e != nil {
		err = e
		return
	}
	return
}

func TaskFromContext(ctx context.Context) *Task {
	return ctx.Value("Task").(*Task)
}
