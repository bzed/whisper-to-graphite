package main

import (
	"errors"
	"flag"
	"github.com/bzed/go-whisper"
	"github.com/marpaia/graphite-golang"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

func sendWhisperData(
	filename string,
	baseDirectory string,
	graphiteConn *graphite.Graphite,
	fromTs int,
	toTs int,
) error {
	metricName, err := convertFilename(filename, baseDirectory)
	if err != nil {
		return err
	}

	whisperData, err := whisper.Open(filename)
	if err != nil {
		return err
	}

	archiveDataPoints, err := whisperData.DumpArchives()
	if err != nil {
		return err
	}
	metrics := make([]graphite.Metric, 0, 1000)
	for _, dataPoint := range archiveDataPoints {
		interval, value := dataPoint.Point()

		if math.IsNaN(value) || interval == 0 || interval < fromTs || interval > toTs {
			continue
		}

		v := strconv.FormatFloat(value, 'f', 20, 64)
		metrics = append(metrics, graphite.NewMetric(metricName, v, int64(interval)))

	}
	err = graphiteConn.SendMetrics(metrics)
	if err != nil {
		return err
	}
	err = nil
	return err
}

func findWhisperFiles(ch chan string, quit chan int, directory string) {
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
	close(quit)
}

func worker(ch chan string,
	quit chan int,
	wg *sync.WaitGroup,
	baseDirectory string,
	graphiteHost string,
	graphitePort int,
	graphiteProtocol string,
	fromTs int,
	toTs int) {

	defer wg.Done()

	graphiteConn, err := graphite.GraphiteFactory(graphiteProtocol, graphiteHost, graphitePort, "")
	if err != nil {
		return
	}

	for {
		select {
		case path := <-ch:
			{

				err := sendWhisperData(path, baseDirectory, graphiteConn, fromTs, toTs)
				if err != nil {
					log.Println("Failed: " + path)
					log.Println(err)
				} else {
					log.Println("OK: " + path)
				}
			}
		case <-quit:
			return
		}

	}
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
	workers := flag.Int(
		"workers",
		5,
		"Workers to run in parallel")
	fromTs := flag.Int(
		"from",
		0,
		"Starting timestamp to dump data from")
	toTs := flag.Int(
		"to",
		2147483647,
		"Ending timestamp to dump data up to")
	flag.Parse()

	if !(*graphiteProtocol == "tcp" ||
		*graphiteProtocol == "udp" ||
		*graphiteProtocol == "nop") {
		log.Fatalln("Graphite protocol " + *graphiteProtocol + " not supported, use tcp/udp/nop.")
	}
	ch := make(chan string)
	quit := make(chan int)
	var wg sync.WaitGroup

	wg.Add(*workers)
	for i := 0; i < *workers; i++ {
		go worker(ch, quit, &wg, *baseDirectory, *graphiteHost, *graphitePort, *graphiteProtocol, *fromTs, *toTs)
	}
	go findWhisperFiles(ch, quit, *directory)
	wg.Wait()
}
