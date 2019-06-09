package fangtooth

import (
	"errors"
	"fmt"

	"github.com/Kamva/shark/exceptions"
	"github.com/Kamva/shark/sentry"
	"github.com/getsentry/raven-go"
	"github.com/gocraft/work"
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
	fmt.Printf("Starting job from queue %s with ID %s\r\n", job.Name, job.ID)
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
			} else {
				reportMessage = fmt.Sprint(err)
				reportTags = map[string]string{"exceptions": "unknown", "type": fmt.Sprintf("%T", err)}
			}

			packet := raven.NewPacket(
				"Notification Worker Error: "+reportMessage,
				raven.NewException(errors.New(reportMessage), raven.NewStacktrace(2, 3, nil)),
			)
			raven.Capture(packet, reportTags)
		}
	}()

	err := next()

	if err != nil {
		sentry.CaptureError(err, map[string]string{"worker": "true", "job": job.ID})
	}

	return err
}
