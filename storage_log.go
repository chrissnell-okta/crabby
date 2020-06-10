package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

// LogConfig describes the YAML-provided configuration for a Console
// storage backend
type LogConfig struct {
	Stream string `yaml:"stream"`
}

type LogStorage struct {
	Stream *os.File
}

// StartStorageEngine creates a goroutine loop to receive metrics and send
// them off to Log
func (c LogStorage) StartStorageEngine(ctx context.Context, wg *sync.WaitGroup) (chan<- Metric, chan<- Event) {
	metricChan := make(chan Metric, 10)
	eventChan := make(chan Event, 10)

	// Start processing the metrics we receive
	go c.processMetricsAndEvents(ctx, wg, metricChan, eventChan)

	return metricChan, eventChan
}

func (c LogStorage) processMetricsAndEvents(ctx context.Context, wg *sync.WaitGroup, mchan <-chan Metric, echan <-chan Event) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case m := <-mchan:
			err := c.sendMetric(m)
			if err != nil {
				log.Println(err)
			}
		case e := <-echan:
			err := c.sendEvent(e)
			if err != nil {
				log.Println(err)
			}
		case <-ctx.Done():
			log.Println("Cancellation request recieved.  Cancelling metrics processor.")
			c.Stream.Close()
			return
		}
	}
}

// sendMetric sends a metric value to the Console
func (c LogStorage) sendMetric(m Metric) error {

	fmt.Fprintf(c.Stream, "Job: %s, Timing: %s, Value: %.6g, Time: %s, URL: %s\n", m.Job, m.Timing, m.Value,
		m.Timestamp.Local().Format("Jan _2 15:04:05"), m.URL)
	return nil
}

// sendEvent sends an event to the Console
func (c LogStorage) sendEvent(e Event) error {
	fmt.Fprintf(c.Stream, "Job: %s, ServerStatus: %d, Time: %s\n", e.Name, e.ServerStatus, e.Timestamp)
	return nil
}

func NewLogStorage(c *Config) (LogStorage, error) {
	var outStream *os.File
	switch c.Storage.Log.Stream {
	case "stdout":
		outStream = os.Stdout
	case "stderr":
		outStream = os.Stderr
	case "stdin":
		outStream = os.Stdin
	default:
		var err error
		outStream, err = os.OpenFile(c.Storage.Log.Stream, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		// Don't defer fileStream close until processor is cancelled.
		if err != nil {
			return LogStorage{Stream: outStream}, err
		}
	}
	return LogStorage{Stream: outStream}, nil
}
