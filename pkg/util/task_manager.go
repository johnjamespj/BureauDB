package util

import (
	"time"
)

type Runnable interface {
	Run()
}

type TaskManager interface {
	Run()
	AddTask(task Runnable)
	AddPeriodicTask(task Runnable, interval time.Duration)
}

type SimpleTaskRunner struct {
	tasks           chan Runnable
	maxRunningTasks int
	runnerTokens    chan bool
}

func NewSimpleTaskRunner(maxRunningTasks int) *SimpleTaskRunner {
	return &SimpleTaskRunner{
		tasks:           make(chan Runnable, 100),
		maxRunningTasks: maxRunningTasks,
		runnerTokens:    make(chan bool, maxRunningTasks),
	}
}

func (r *SimpleTaskRunner) Run() {
	for i := 0; i < r.maxRunningTasks; i++ {
		r.runnerTokens <- true
		go r.run()
	}
}

func (r *SimpleTaskRunner) run() {
	for {
		task := <-r.tasks
		<-r.runnerTokens
		task.Run()
		r.runnerTokens <- true
	}
}

func (r *SimpleTaskRunner) AddTask(task Runnable) {
	r.tasks <- task
}

func (r *SimpleTaskRunner) AddPeriodicTask(task Runnable, interval time.Duration) {
	go func() {
		for {
			<-r.runnerTokens
			task.Run()
			r.runnerTokens <- true
			time.Sleep(interval)
		}
	}()
}
