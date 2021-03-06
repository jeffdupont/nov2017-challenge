package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type logfile struct {
	count   int
	handler *os.File
}

func (l *logfile) getFilename() string {
	return fmt.Sprintf("data.%d.log", l.count)
}

func (l *logfile) Close() error {
	return l.handler.Close()
}

func (l *logfile) create() error {
	if l.handler != nil {
		l.handler.Close()
	}
	file, err := os.OpenFile(l.getFilename(), os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		log.Fatalf("could not open file: %v\n", err)
	}
	l.handler = file
	return nil
}

func (l *logfile) write(s string) error {
	_, err := l.handler.WriteString(s + "\n")
	if err != nil {
		return err
	}
	return nil
}

const maxConnections = 6

// Store saves all metric data and relies on the RW Mutex to ensure
// that all metric names are distinct. I used RW to allow concurrent reads
// when check that the key exists before locking to save the metric
type store struct {
	data map[string]int
}

// Update checks to see if the metric key exists
// and then updates the existing value before it is saved
// back to the data store
func (s *store) update(v string) (string, error) {
	// check if the metric exists
	if _, ok := s.data[v]; ok {
		s.data[v] = s.data[v] + 1
	} else {
		s.data[v] = 1
	}
	if s.data[v] == 1 {
		return v, nil
	}
	return "", nil
}

type empty struct{}
type semaphore chan empty

// acquire n resources
func (s semaphore) process(n int) {
	e := empty{}
	for i := 0; i < n; i++ {
		s <- e
	}
}

// release n resources
func (s semaphore) release(n int) {
	for i := 0; i < n; i++ {
		<-s
	}
}

func (s semaphore) Signal() {
	s.release(1)
}
func (s semaphore) Wait(n int) {
	s.process(n)
}

// Initializes the store db for the metric data
func newStore() *store {
	return &store{make(map[string]int)}
}

var (
	rawCount    uint64
	periodCount uint64
)

// Make sure the name contains only valid characters
func validate(str string) bool {
	if len(str) > 10 {
		fmt.Println(str, "invalid input: too big")
		return false
	}
	num, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		fmt.Println(str, "invalid input: not a valid integer")
		return false
	}
	if num < 1000000 {
		fmt.Println(str, "invalid input: less than 1000000")
		return false
	}
	return true
}

// Parse the input line
func parseMetric(line string) (string, error) {
	// validate name
	if ok := validate(line); !ok {
		return "", fmt.Errorf("invalid input")
	}

	return line, nil
}

func main() {
	lf := logfile{count: 0}
	lf.create()

	// initialize the main store db
	store := newStore()

	ingress := make(chan string)
	sd := make(chan empty)

	// establish the tcp listener
	l, err := net.Listen("tcp", ":3280")
	if err != nil {
		log.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	// process feed and tickers
	go func() {
		tickerFive := time.NewTicker(time.Second * 5)
		tickerTen := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-sd:
				fmt.Println("Closing server...")
				// close any remaining file handlers
				lf.Close()
				// close the listening server
				l.Close()
				// duh
				os.Exit(0)
			case m := <-ingress:
				name, _ := store.update(m)
				if name != "" {
					// fmt.Println(name)
					err = lf.write(name)
					if err != nil {
						log.Fatalf("could not write %s: %v\n", name, err)
					}
				}
			case <-tickerFive.C:
				fmt.Fprintf(os.Stderr, "(5 sec): Uniques %d\tPeriod %d\tTotal count %d\n", len(store.data), atomic.LoadUint64(&periodCount), atomic.LoadUint64(&rawCount))
				atomic.StoreUint64(&periodCount, 0) // reset the count
			case <-tickerTen.C:
				lf.count++
				lf.create()
			}
		}
	}()

	sem := make(semaphore, maxConnections)
	for {
		sem.Wait(1)
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Connection: %v\n", err)
			continue
		}
		go connHandler(conn, sem, ingress, sd)
	}
}

func connHandler(conn net.Conn, s semaphore, ingress chan string, shutdown chan empty) {
	defer s.Signal()
	reader := bufio.NewReader(conn)

	for {
		// read the input
		b, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Fprintln(os.Stderr, "client terminated: EOF")
				conn.Close()
				return
			}
		}

		// trim off unnecessary chars
		line := string(bytes.Trim(b, "\r\n"))
		if line == "" {
			fmt.Fprintln(os.Stderr, "client terminated: Empty input")
			conn.Close()
			return
		}

		// if shutdown is requested, then
		// send a trigger down the channel to
		// shutdown the connect and end the server
		if line == "shutdown" {
			shutdown <- empty{}
			return
		}

		// parse the metric
		metric, err := parseMetric(line)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			conn.Close()
			return
		}

		// save the metric to the store
		ingress <- metric

		// increment our total counter for the time the server is running
		atomic.AddUint64(&rawCount, 1)

		// increment total for the given period
		atomic.AddUint64(&periodCount, 1)
	}
}
