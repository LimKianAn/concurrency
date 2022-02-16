package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Traffic struct {
	Timestamp time.Time `json:"time" yaml:"time"`
	Protocol  string    `json:"proto" yaml:"proto"`
}

type threadSafeCount struct {
	sync.Mutex
	count int
}

func (c *threadSafeCount) increment() {
	c.Mutex.Lock()
	c.count++
	c.Mutex.Unlock()
}

func main() {
	fmt.Println("atomic", tcpTrafficCountAtomic(os.Args[1:]))
	fmt.Println("channel", tcpTrafficCountCH(os.Args[1:]))
	fmt.Println("mutex", tcpTrafficCountMutex(os.Args[1:]))
}

func handleError(err error) {
	if err != nil {
		log.Print(err)
		return
	}
}

func tcpTrafficCountAtomic(paths []string) int {
	wg := sync.WaitGroup{}
	wg.Add(len(paths))
	count := int64(0)
	for _, path := range paths {
		go func(path string) {
			bb, err := os.ReadFile(path)
			handleError(err)

			dec := json.NewDecoder(bytes.NewReader(bb))
			for dec.More() {
				tr := Traffic{}
				handleError(dec.Decode(&tr))
				if tr.Protocol == "TCP" {
					atomic.AddInt64(&count, 1)
				}
			}
			wg.Done()
		}(path)
	}
	wg.Wait()
	return int(count)
}

func tcpTrafficCountCH(paths []string) int {
	wg := sync.WaitGroup{}
	wg.Add(len(paths))
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()

	for _, path := range paths {
		go func(path string) {
			bb, err := os.ReadFile(path)
			handleError(err)

			dec := json.NewDecoder(bytes.NewReader(bb))
			for dec.More() {
				tr := Traffic{}
				handleError(dec.Decode(&tr))
				if tr.Protocol == "TCP" {
					ch <- struct{}{}
				}
			}
			wg.Done()
		}(path)
	}

	count := 0
	for range ch {
		count++
	}
	return count
}

func tcpTrafficCountMutex(paths []string) int {
	wg := sync.WaitGroup{}
	wg.Add(len(paths))
	count := threadSafeCount{}
	for _, path := range paths {
		go func(path string) {
			bb, err := os.ReadFile(path)
			handleError(err)

			dec := json.NewDecoder(bytes.NewReader(bb))
			for dec.More() {
				tr := Traffic{}
				handleError(dec.Decode(&tr))
				if tr.Protocol == "TCP" {
					count.increment()
				}
			}
			wg.Done()
		}(path)
	}
	wg.Wait()
	return count.count
}
