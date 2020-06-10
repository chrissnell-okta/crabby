package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// LogConfig describes the YAML-provided configuration for a Console
// storage backend
type LogConfig struct {
	Stream string       `yaml:"stream"`
	Format FormatConfig `yaml:"format"`
	Time   TimeConfig   `yaml:"time"`
}

type FormatConfig struct {
	Metric       string `yaml:"metric"`
	Event        string `yaml:"event"`
	Tag          string `yaml:"tag"`
	TagSeparator string `yaml:"tag-seperator"`
}

type TimeConfig struct {
	Location string `yaml:"location"`
	Format   string `yaml:"format"`
}

type LogStorage struct {
	Stream     *os.File
	Format     FormatConfig
	Location   *time.Location
	TimeFormat string
}

// StartStorageEngine creates a goroutine loop to receive metrics and send
// them off to Log
func (l LogStorage) StartStorageEngine(ctx context.Context, wg *sync.WaitGroup) (chan<- Metric, chan<- Event) {
	metricChan := make(chan Metric, 10)
	eventChan := make(chan Event, 10)

	// Start processing the metrics we receive
	go l.processMetricsAndEvents(ctx, wg, metricChan, eventChan)

	return metricChan, eventChan
}

func (l LogStorage) processMetricsAndEvents(ctx context.Context, wg *sync.WaitGroup, mchan <-chan Metric, echan <-chan Event) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case m := <-mchan:
			err := l.sendMetric(m)
			if err != nil {
				log.Println(err)
			}
		case e := <-echan:
			err := l.sendEvent(e)
			if err != nil {
				log.Println(err)
			}
		case <-ctx.Done():
			log.Println("Cancellation request recieved.  Cancelling metrics processor.")
			l.Stream.Close()
			return
		}
	}
}

func (l LogStorage) BuildTagFormatString(tags map[string]string) string {

	if len(tags) == 0 {
		return ""
	}

	var sb strings.Builder
	for name, value := range tags {
		replacer := strings.NewReplacer(
			"%name", name,
			"%value", value,
		)
		sb.WriteString(replacer.Replace(l.Format.Tag))
		sb.WriteString(l.Format.TagSeparator)
	}
	return strings.TrimSuffix(sb.String(), l.Format.TagSeparator)
}

func (l LogStorage) BuildMetricFormatString(m Metric) string {
	replacer := strings.NewReplacer(
		"%job", m.Job,
		"%timing", m.Timing,
		"%value", fmt.Sprintf("%.6g", m.Value),
		"%time", m.Timestamp.In(l.Location).Format(l.TimeFormat),
		"%url", m.URL,
		"%tags", l.BuildTagFormatString(m.Tags))
	return replacer.Replace(l.Format.Metric)
}

func (l LogStorage) BuildEventFormatString(e Event) string {
	replacer := strings.NewReplacer(
		"%name", e.Name,
		"%status", fmt.Sprint(e.ServerStatus),
		"%time", e.Timestamp.In(l.Location).Format(l.TimeFormat),
		"%tags", l.BuildTagFormatString(e.Tags))
	return replacer.Replace(l.Format.Event)
}

// sendMetric sends a metric value to the Console
func (l LogStorage) sendMetric(m Metric) error {
	_, err := l.Stream.WriteString(l.BuildMetricFormatString(m))
	if err != nil {
		return err
	}
	return nil
}

// sendEvent sends an event to the Console
func (l LogStorage) sendEvent(e Event) error {
	_, err := l.Stream.WriteString(l.BuildEventFormatString(e))
	if err != nil {
		return err
	}
	return nil
}

func NewLogStorage(c *Config) (LogStorage, error) {
	var outStream *os.File
	var l = LogStorage{}

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
			return l, err
		}
	}

	location, err := time.LoadLocation(c.Storage.Log.Time.Location)
	if err != nil {
		return l, err
	}

	l.Stream = outStream
	l.TimeFormat = c.Storage.Log.Time.Format
	l.Location = location
	l.Format = c.Storage.Log.Format

	return l, nil
}
