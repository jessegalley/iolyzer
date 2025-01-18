package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/spf13/pflag"
)

// // Job represents a simulated IO operation
// type Job struct {
// 	// simulated io latency in nanoseconds
// 	latency time.Duration
// }
// "vectorize" the Jobs
type Job struct {
    latency time.Duration
    iterations int  // number of times to repeat the operation
}

func main() {
	// define command line flags
	workers := pflag.Int("workers", 8, "number of worker goroutines")
	buffer := pflag.Int("buffer", 8, "size of job buffer")
	duration := pflag.Duration("duration", 15*time.Second, "test duration")
	latency := pflag.Duration("latency", 50*time.Microsecond, "simulated io latency")
	
	// parse command line flags
	pflag.Parse()

	// calculate theoretical maximum jobs/sec
	theoretical := float64(*workers) * (float64(time.Second) / float64(*latency))

	// create job channel
	jobChan := make(chan Job, *buffer)
	done := make(chan struct{})

	// create wait group for workers
	var wg sync.WaitGroup

	// slice to collect results from workers
	results := make(chan int)

	// start workers
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(jobChan, &wg, results)
	}

	// start producer
	start := time.Now()
	go producer(jobChan, done, *latency)

	// wait for test duration
	time.Sleep(*duration)
	close(done)

	// wait for all workers and close results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	var totalJobs int
	for count := range results {
		totalJobs += count
	}

	elapsed := time.Since(start).Seconds()
	actual := float64(totalJobs) / elapsed
	efficiency := (actual / theoretical) * 100

	// print results
	fmt.Printf("\nResults:\n")
	fmt.Printf("Theoretical max jobs/sec: %.2f\n", theoretical)
	fmt.Printf("Actual jobs/sec: %.2f\n", actual)
	fmt.Printf("Efficiency: %.2f%%\n", efficiency)
}

// worker processes jobs and returns total count when done
func worker(jobs <-chan Job, wg *sync.WaitGroup, results chan<- int) {
    defer wg.Done()
    count := 0

    for {
        select {
        case job, ok := <-jobs:
            if !ok {
                results <- count
                return
            }
            // perform operation multiple times before getting next job
            for i := 0; i < job.iterations; i++ {
                time.Sleep(job.latency)
                count++
            }
        }
    }
}

// producer generates jobs until done channel is closed
func producer(jobs chan<- Job, done <-chan struct{}, latency time.Duration) {
    blocked := 0
    iterations := 10// or whatever batch size we want to test

    for {
        select {
        case <-done:
            fmt.Printf("Producer blocked %d times\n", blocked)
            close(jobs)
            return
        case jobs <- Job{latency: latency, iterations: iterations}:
            // sent successfully
        default:
            blocked++
            select {
            case jobs <- Job{latency: latency, iterations: iterations}:
            case <-done:
                fmt.Printf("Producer blocked %d times\n", blocked)
                close(jobs)
                return
            }
        }
    }
}
