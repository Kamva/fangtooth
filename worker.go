package fangtooth

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Kamva/shark/exceptions"
	"github.com/Kamva/shark/sentry"
	"github.com/getsentry/raven-go"
	"github.com/gocraft/work"
	"github.com/kataras/golog"
)

// WorkerInterface is an interface for workers contexts.
type WorkerInterface interface {
	Log(*work.Job, work.NextMiddlewareFunc) error
	CaptureError(*work.Job, work.NextMiddlewareFunc) error
	Self() interface{}
}

// WorkerContext is base worker context
type WorkerContext struct{}

// Log is middleware that log the currently being processed job.
func (w *WorkerContext) Log(job *work.Job, next work.NextMiddlewareFunc) error {
	golog.Infof("Starting job from queue %s with ID %s", job.Name, job.ID)
	return next()
}

// CaptureError is a middleware for reporting panics and error in workers.
func (w *WorkerContext) CaptureError(job *work.Job, next work.NextMiddlewareFunc) error {
	defer func() {
		if err := recover(); err != nil {
			var reportMessage string
			var reportTags map[string]string

			if e, ok := err.(exceptions.GenericException); ok {
				reportMessage = e.GetErrorMessage()
				reportTags = e.GetTags()
				golog.Warn(err)
			} else {
				reportMessage = fmt.Sprint(err)
				reportTags = map[string]string{"exceptions": "unknown", "type": fmt.Sprintf("%T", err)}
			}

			w.tagJobInfo(reportTags, job)

			packet := raven.NewPacket(
				"Worker Error: "+reportMessage,
				raven.NewException(errors.New(reportMessage), raven.NewStacktrace(2, 3, nil)),
			)

			golog.Error("[PANIC] " + packet.Message)

			raven.Capture(packet, reportTags)
		}
	}()

	err := next()

	if err != nil {
		golog.Error(err.Error())
		tags := map[string]string{"worker": "true"}

		w.tagJobInfo(tags, job)

		sentry.CaptureError(err, tags)
	} else {
		golog.Infof("Job %s with ID %s proceed successfully.", job.Name, job.ID)
	}

	return err
}

func (w *WorkerContext) tagJobInfo(tags map[string]string, job *work.Job) {
	tags["job"] = job.Name
	tags["job_id"] = job.ID
	tags["fails"] = strconv.Itoa(int(job.Fails))
}
