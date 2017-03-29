package main

import (
	"errors"
	"flag"
	"github.com/lomik/go-whisper"
	"github.com/marpaia/graphite-golang"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func convertFilename(filename string, baseDirectory string) (string, error) {
	absFilename, err := filepath.Abs(filename)
	if err != nil {
		return "", err
	}
	absBaseDirectory, err := filepath.Abs(baseDirectory)
	if err != nil {
		return "", err
	}
	err = nil
	if strings.HasPrefix(absFilename, absBaseDirectory) {
		metric := strings.Replace(
			strings.TrimPrefix(
				strings.TrimSuffix(
					strings.TrimPrefix(
						absFilename,
						absBaseDirectory),
					".wsp"),
				"/"),
			"/",
			".",
			-1)
		return metric, err
	}
	err = errors.New("Path for whisper file does not live in BasePath")
	return "", err
}

func sendWhisperData(filename string, baseDirectory string, graphiteHost string, graphitePort int, graphiteProtocol string) error {
	metricName, err := convertFilename(filename, baseDirectory)
	if err != nil {
		return err
	}

	graphiteConn, err := graphite.GraphiteFactory(graphiteProtocol, graphiteHost, graphitePort, "")
	if err != nil {
		return err
	}

	whisperData, err := whisper.Open(filename)
	if err != nil {
		return err
	}

	timeSeriesdata, err := whisperData.Fetch(whisperData.StartTime(), int(time.Now().Unix()))
	if err != nil {
		return err
	}

	point_count := 0
	metrics := make([]graphite.Metric, 1000)
	for _, point := range timeSeriesdata.Points() {
		if math.IsNaN(point.Value) {
			continue
		}
		v := strconv.FormatFloat(point.Value, 'f', -1, 64)
		metrics[point_count] = graphite.NewMetric(metricName, v, int64(point.Time))
		point_count++
		if point_count >= 1000 {
			err = graphiteConn.SendMetrics(metrics)
			if err != nil {
				return err
			}
			point_count = 0
			metrics = make([]graphite.Metric, 1000)
		}
	}
	err = graphiteConn.SendMetrics(metrics)
	if err != nil {
		return err
	}

	err = nil
	return err
}

func findWhisperFiles(ch chan string, directory string) {
	visit := func(path string, info os.FileInfo, err error) error {
		if (info != nil) && !info.IsDir() {
			if strings.HasSuffix(path, ".wsp") {
				ch <- path
			}
		}
		return nil
	}
	err := filepath.Walk(directory, visit)
	if err != nil {
		log.Fatal(err)
	}
	close(ch)
}

func main() {

	baseDirectory := flag.String(
		"basedirectory",
		"/var/lib/graphite/whisper",
		"Base directory where whisper files are located. Used to retrieve the metric name from the filename.")
	directory := flag.String(
		"directory",
		"/var/lib/graphite/whisper/collectd",
		"Directory containing the whisper files you want to send to graphite again")
	graphiteHost := flag.String(
		"host",
		"127.0.0.1",
		"Hostname/IP of the graphite server")
	graphitePort := flag.Int(
		"port",
		2003,
		"graphite Port")
	graphiteProtocol := flag.String(
		"protocol",
		"tcp",
		"Protocol to use to transfer graphite data (tcp/udp/nop)")
	flag.Parse()

	ch := make(chan string)
	go findWhisperFiles(ch, *directory)
	for wspFile := range ch {
		sendWhisperData(wspFile, *baseDirectory, *graphiteHost, *graphitePort, *graphiteProtocol)
	}
}
