//
// main.go
//
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/k4k3ru-hub/worker-pool/go"
)


type Job struct {
    ID   int
    Body string
}


type Worker struct{}

func (w *Worker) Work(ctx context.Context, jobs []Job) error {
    // Simulate batch work.
    fmt.Printf("[worker] batch_len=%d\n", len(jobs))
    for _, j := range jobs {
        select {
        case <-ctx.Done():
            fmt.Println("[worker] canceled")
            return ctx.Err()
        default:
        }
        fmt.Printf("  - job id=%d body=%q\n", j.ID, j.Body)
    }
    return nil
}


//
// Main.
//
func main() {
    // Create a pool.
    p, err := workerpool.NewPool(workerpool.PoolOption{
        QueueSize:    16,
        NumWorkers:   2,
        PollInterval: 50 * time.Millisecond,
        BatchSize:    4,
    }, &Worker{})
    if err != nil {
        panic(err)
    }

    // Start.
    if err := p.Start(); err != nil {
        panic(err)
    }

    // Enqueue some jobs.
    for i := 1; i <= 10; i++ {
        _ = p.Enqueue(Job{ID: i, Body: fmt.Sprintf("hello-%d", i)})
    }

    time.Sleep(300 * time.Millisecond)

    // Stop (clear queue, no wait for in-flight work).
    if err := p.Stop(); err != nil {
        panic(err)
    }
    fmt.Println("[main] stopped pool")

    // Enqueue after Stop => should fail.
    if err := p.Enqueue(Job{ID: 999, Body: "should fail"}); err != nil {
        fmt.Println("[main] enqueue after stop:", err.Error())
    }

    // Re-start (new context).
    if err := p.StartWithContext(context.Background()); err != nil {
        panic(err)
    }
    fmt.Println("[main] restarted pool")

    // Enqueue again.
    for i := 11; i <= 15; i++ {
        _ = p.Enqueue(Job{ID: i, Body: fmt.Sprintf("again-%d", i)})
    }

    time.Sleep(300 * time.Millisecond)

    _ = p.Stop()
    fmt.Println("[main] done")
}


