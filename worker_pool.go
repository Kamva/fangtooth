package fangtooth

import (
	"os"
	"os/signal"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
)

// Handler is a function to process a job.
type Handler func(*work.Job) error

// Middleware is a function works as a middleware.
type Middleware func(*work.Job, work.NextMiddlewareFunc) error

// Configurator is a function that accept worker pool as parameter and change
// its configurable attributes.
type Configurator func(*WorkerPool)

// WorkerPool is responsible for fetching jobs out from redis pool and
// dispatching them to workers.
type WorkerPool struct {
	ConcurrentProcess uint
	namespace         string
	redisPool         *redis.Pool
	pool              *work.WorkerPool
}

// Middleware will add given middleware to process pipeline.
func (p *WorkerPool) Middleware(m Middleware) *WorkerPool {
	p.pool.Middleware(m)

	return p
}

// Listen will listens for incoming job with specified job name and handle it
// with its handler function.
func (p *WorkerPool) Listen(jobName string, handler Handler) *WorkerPool {
	p.pool.Job(jobName, handler)

	return p
}

// Run starts worker pool and starts processing jobs.
func (p *WorkerPool) Run() {
	p.pool.Start()

	// Wait for a signal to quit:
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan

	p.pool.Stop()
}

// NewWorkerPool construct new instance worker pool.
func NewWorkerPool(worker WorkerInterface, namespace string, redisPool *redis.Pool, configurators ...Configurator) *WorkerPool {
	wp := &WorkerPool{namespace: namespace, redisPool: redisPool}

	// run configurators to set custom value for model attributes
	for _, configure := range configurators {
		configure(wp)
	}

	// check for concurrent process number setting: set it to default if not exist.
	if wp.ConcurrentProcess == 0 {
		wp.ConcurrentProcess = 1
	}

	wp.pool = work.NewWorkerPool(worker, wp.ConcurrentProcess, wp.namespace, wp.redisPool)

	return wp
}
