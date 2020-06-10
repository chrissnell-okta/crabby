package main

import (
	"context"
	"log"
	"sync"
)

type ConsoleStorage struct {
}

// StartStorageEngine creates a goroutine loop to receive metrics and send
// them off to Graphite
func (c ConsoleStorage) StartStorageEngine(ctx context.Context, wg *sync.WaitGroup) (chan<- Metric, chan<- Event) {
	// We're going to declare eventChan here but not initialize the channel because Graphite
	// storage doesn't support Events, only Metrics.  We should never receive an Event on this
	// channel and if something mistakenly sends one, the program will panic.
	var eventChan chan<- Event

	// We *do* support Metrics, so we'll initialize this as a buffered channel
	metricChan := make(chan Metric, 10)

	// Start processing the metrics we receive
	go c.processMetrics(ctx, wg, metricChan)

	return metricChan, eventChan
}

func (c ConsoleStorage) processMetrics(ctx context.Context, wg *sync.WaitGroup, mchan <-chan Metric) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case m := <-mchan:
			err := c.sendMetric(m)
			if err != nil {
				log.Println(err)
			}
		case <-ctx.Done():
			log.Println("Cancellation request recieved.  Cancelling metrics processor.")
			return
		}
	}
}

// sendMetric sends a metric value to InfluxDB
func (c ConsoleStorage) sendMetric(m Metric) error {
	// var metricName string
	return nil
}

// sendEvent is necessary to implement the StorageEngine interface.
func (c ConsoleStorage) sendEvent(e Event) error {
	var err error
	return err
}

func NewConsoleStorage(c *Config) ConsoleStorage {
	return ConsoleStorage{}
}
