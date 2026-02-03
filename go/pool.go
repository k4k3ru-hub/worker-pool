//
// pool.go
//
package workerpool

import (
    "context"
    "fmt"
    "sync"
    "time"
)


type Worker[T any] interface {
    Work(ctx context.Context, jobs []T) error
}

type Pool[T any] struct {
    worker       Worker[T]
    ctx          context.Context
    cancel       context.CancelFunc
    pingCh       chan struct{}
    workerCh     chan struct{}
    mu           sync.Mutex
    jobPool      []T
    jobPoolHead  int
    jobPoolTail  int
    jobPoolSize  int
    batchSize    int
    pollInterval time.Duration
}

type PoolOption struct {
    QueueSize    int
    NumWorkers   int
    PollInterval time.Duration
    BatchSize    int
}


//
// Creates a Pool instance (not started).
// It allocates the ring buffer and channels based on options.
//
// Version:
//   - 2026-02-01: Created new.
//
func NewPool[T any](opt PoolOption, worker Worker[T]) (*Pool[T], error) {
    // Guard.
    if worker == nil {
        return nil, fmt.Errorf("failed to create pool: worker=nil")
    }

    // Set defaults.
	if opt.QueueSize <= 0 {
		opt.QueueSize = 1024
	}
	if opt.BatchSize <= 0 {
		opt.BatchSize = 128
	}
    if opt.BatchSize > opt.QueueSize {
        opt.BatchSize = opt.QueueSize
    }
	if opt.NumWorkers <= 0 {
		opt.NumWorkers = 1
	}
	if opt.PollInterval <= 0 {
		opt.PollInterval = 25 * time.Millisecond
	}

    // Create a pool.
    p := &Pool[T]{
		worker:       worker,
		pingCh:       make(chan struct{}, 1),
		workerCh:     make(chan struct{}, opt.NumWorkers),
		jobPool:      make([]T, opt.QueueSize),
		jobPoolHead:  0,
		jobPoolTail:  0,
		jobPoolSize:  0,
		batchSize:    opt.BatchSize,
		pollInterval: opt.PollInterval,
	}

	return p, nil
}


//
// Enqueue a job to the queue (non-blocking).
//
// Version:
//   - 2026-02-02: Created new.
//
func (p *Pool[T]) Enqueue(job T) error {
    p.mu.Lock()
    defer p.mu.Unlock()

    // Check if the workerpool has been started or not.
    if p.ctx == nil {
        return fmt.Errorf("failed to enqueue because the workerpool is not started.")
    }
    if err := p.ctx.Err(); err != nil {
        return fmt.Errorf("failed to enqueue because the workerpool is closed: %w", err)
    }

    // Check if the queue must have capacity.
    if p.jobPoolSize >= len(p.jobPool) {
        return fmt.Errorf("failed to enqueue because the workerpool queue is full.")
    }

    // Push to ring buffer (FIFO).
    p.jobPool[p.jobPoolTail] = job
    p.jobPoolTail++
    if p.jobPoolTail == len(p.jobPool) {
        p.jobPoolTail = 0
    }
    p.jobPoolSize++

    // Ping (non-blocking) to wake up runWorker.
    select {
    case p.pingCh <- struct{}{}:
    default:
    }

    return nil
}


//
// Enqueue multiple jobs to the queue at once (non-blocking).
//
// Version:
//   - 2026-02-02: Created new.
//
func (p *Pool[T]) EnqueueBatch(jobs []T) error {
    // Guard.
    if len(jobs) == 0 {
        return nil
    }

    p.mu.Lock()
    defer p.mu.Unlock()

    // Check if the workerpool has been started or not.
    if p.ctx == nil {
        return fmt.Errorf("failed to enqueue because the workerpool is not started.")
    }
    if err := p.ctx.Err(); err != nil {
        return fmt.Errorf("failed to enqueue because the workerpool is closed: %w", err)
    }

    // Check if the queue must have capacity.
    capLeft := len(p.jobPool) - p.jobPoolSize
    if len(jobs) > capLeft {
        return fmt.Errorf("failed to enqueue because the workerpool queue is full.")
    }

    // Push to ring buffer (FIFO).
    for i := 0; i < len(jobs); i++ {
        p.jobPool[p.jobPoolTail] = jobs[i]
        p.jobPoolTail++
        if p.jobPoolTail == len(p.jobPool) {
            p.jobPoolTail = 0
        }
        p.jobPoolSize++
    }

    // Ping (non-blocking) to wake up runWorker.
    select {
    case p.pingCh <- struct{}{}:
    default:
    }

    return nil
}


//
// Starts the pool using context.Background() as the parent.
//
// Version:
//   - 2026-02-02: Created new.
//
func (p *Pool[T]) Start() error {
    return p.StartWithContext(context.Background())
}


//
// Starts the pool using the given parent context.
//
// Version:
//   - 2026-02-02: Created new.
//
func (p *Pool[T]) StartWithContext(parentCtx context.Context) error {
    // Guard.
    if parentCtx == nil {
        return fmt.Errorf("failed to start workerpool: parent_ctx=null")
    }

    // Check if the workerpool has been already started or not.
    p.mu.Lock()
    if p.ctx != nil {
        p.mu.Unlock()
        return fmt.Errorf("skip starting workerpool because it has been already started.")
    }

    ctx, cancel := context.WithCancel(parentCtx)
    p.ctx = ctx
    p.cancel = cancel
    p.mu.Unlock()

    // Start worker routine.
    go p.runWorker(ctx)

    return nil
}


//
// Stops the pool (idempotent).
// It cancels the internal context and waits for in-flight workers to finish.
//
// Version:
//   - 2026-02-02: Created new.
//
func (p *Pool[T]) Stop() error {
    p.mu.Lock()

    // Check if the workerpool has been already started or not.
    if p.ctx == nil {
        p.mu.Unlock()
        return nil
    }

    // Take a snapshot.
    cancel := p.cancel

    // Clear queued jobs (drop all).
    var zero T
    for i := 0; i < len(p.jobPool); i++ {
        p.jobPool[i] = zero
    }
    p.jobPoolHead = 0
    p.jobPoolTail = 0
    p.jobPoolSize = 0

    // Mark as stopped first (so Enqueue fails immediately).
    p.ctx = nil
    p.cancel = nil

    p.mu.Unlock()

    // Cancel.
    if cancel != nil {
        cancel()
    }
    return nil
}



//
// Starts the workerpool routine.
//
// Version:
//   - 2026-02-02: Created new.
//
func (p *Pool[T]) runWorker(ctx context.Context) {
    // Starts ticker.
    ticker := time.NewTicker(p.pollInterval)
    defer ticker.Stop()

    for {
		select {
		case <-ctx.Done():
			return
		case <-p.pingCh:
			p.dispatchJob(ctx)
		case <-ticker.C:
			p.dispatchJob(ctx)
		}
	}
}


//
// Dispatch queued jobs to workers.
//
func (p *Pool[T]) dispatchJob(ctx context.Context) {
    // Check if the context has already done.
    select {
    case <-ctx.Done():
        return
    default:
    }

    // Acquire semaphore.
    // If no capacity, just return and retry on next ping/ticker.
    select {
    case p.workerCh <- struct{}{}:
    default:
        return
    }

    // Get jobs by FIFO.
    maxBatch := p.batchSize

    p.mu.Lock()

    if p.jobPoolSize == 0 {
        p.mu.Unlock()
        <-p.workerCh
        return
    }

    // Pop jobs from job_pool.
    batchLen := maxBatch
    if p.jobPoolSize < batchLen {
        batchLen = p.jobPoolSize
    }

    jobs := make([]T, 0, batchLen)

    // Manage job_pool_head / job_pool_size.
    for i := 0; i < batchLen; i++ {
        j := p.jobPool[p.jobPoolHead]
        jobs = append(jobs, j)

        // Clear slot to avoid holding references (if Job contains pointers).
        var zero T
        p.jobPool[p.jobPoolHead] = zero

        p.jobPoolHead++

        // Re-set job_pool_head when reaching to the maximum capacity of job_pool.
        if p.jobPoolHead == len(p.jobPool) {
            p.jobPoolHead = 0
        }
    }
    p.jobPoolSize -= batchLen

    p.mu.Unlock()

    go p.work(ctx, jobs)
}


func (p *Pool[T]) work(ctx context.Context, jobs []T) {
    defer func() {
        <-p.workerCh
    }()

    select {
    case <-ctx.Done():
        return
    default:
    }
    _ = p.worker.Work(ctx, jobs)
}

