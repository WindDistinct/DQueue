package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

type Job struct {
	ID        int
	Name      string
	Payload   string
	Status    JobStatus
	Attempts  int
	CreatedAt time.Time
}

type JobStatus string

const (
	StatusPending JobStatus = "pending"
	StatusRunning JobStatus = "running"
	StatusDone    JobStatus = "done"
	StatusFailed  JobStatus = "failed"
)

type Queue struct {
	jobs []*Job
	mu   sync.Mutex
}

var nextID = 1

const MaxRetries = 3

func NewJob(name string, payload string) *Job {
	newJob := &Job{}

	newJob.ID = nextID
	newJob.Name = name
	newJob.Payload = payload
	newJob.Status = StatusPending
	newJob.CreatedAt = time.Now()

	nextID++

	return newJob
}

func (q *Queue) Enqueue(job *Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs = append(q.jobs, job)
}

func (q *Queue) Dequeue() (job *Job, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.jobs) == 0 {
		err = errors.New("La colas está vacía")
		return nil, err
	} else {
		job = q.jobs[0]
		q.jobs = q.jobs[1:]
		return job, nil
	}
}

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var wg sync.WaitGroup

	fmt.Println("DQueue starting...")

	queue := &Queue{}

	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go worker(i, queue, ctx, &wg)
	}

	server := &http.Server{Addr: ":8080"}
	http.HandleFunc("/jobs", enqueueHandler(queue))

	go func() {
		server.ListenAndServe()
	}()

	<-ctx.Done()

	fmt.Println("Señal de apagado recibida...")
	server.Shutdown(context.Background())
	wg.Wait()
	fmt.Println("DQueue apagado correctamente.")
}

func worker(id int, queue *Queue, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Worker %d: apagando...\n", id)
			return

		default:
		}
		job, err := queue.Dequeue()
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		} else {
			job.Status = StatusRunning
			time.Sleep(1 * time.Second)

			failed := rand.Intn(100) < 40

			if failed {
				job.Attempts++
				if job.Attempts < MaxRetries {
					job.Status = StatusPending
					fmt.Printf("Worker %d: Job %d falló reintento %d/%d\n", id, job.ID, job.Attempts, MaxRetries)
					queue.Enqueue(job)
				} else {
					job.Status = StatusFailed
					fmt.Printf("Worker %d: Job %d falló definitivamente %d/%d\n", id, job.ID, job.Attempts, MaxRetries)
				}
			} else {
				job.Status = StatusDone
				fmt.Printf("Worker %d: Job %d se completó exitosamente %d/%d\n", id, job.ID, job.Attempts, MaxRetries)

			}

		}
	}
}

func enqueueHandler(queue *Queue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name    string `json:"name"`
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "JSON inválido", http.StatusBadRequest)
			return
		}
		job := NewJob(req.Name, req.Payload)
		queue.Enqueue(job)
		fmt.Fprintf(w, "Job %d encolado\n", job.ID)
	}
}
