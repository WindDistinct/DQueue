package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Job struct {
	ID        int
	Name      string
	Payload   string
	CreatedAt time.Time
}

type Queue struct {
	jobs []*Job
	mu   sync.Mutex
}

var nextID = 1

func NewJob(name string, payload string) *Job {
	newJob := &Job{}

	newJob.ID = nextID
	newJob.Name = name
	newJob.Payload = payload
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
	fmt.Println("DQueue starting...")

	var wg sync.WaitGroup

	queue := &Queue{}

	queue.Enqueue(NewJob("test 1", "payload 1"))
	queue.Enqueue(NewJob("test 2", "payload 2"))
	queue.Enqueue(NewJob("test 3", "payload 3"))
	queue.Enqueue(NewJob("test 4", "payload 4"))
	queue.Enqueue(NewJob("test 5", "payload 5"))
	queue.Enqueue(NewJob("test 6", "payload 6"))

	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go worker(i, queue, &wg)
	}

	wg.Wait()

	// for i := 0; i < 4; i++ {
	// 	job, err := queue.Dequeue()
	// 	if err != nil {
	// 		fmt.Printf("%d: No hay más Jobs en la cola\n", i+1)
	// 	} else {
	// 		fmt.Printf("Se retiro el Job %d de la cola\n", job.ID)
	// 	}
	// }
}

func worker(id int, queue *Queue, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		job, err := queue.Dequeue()
		if err != nil {
			return
		} else {
			fmt.Printf("Worker %d procesando Job %d (%s)\n", id, job.ID, job.Name)
			time.Sleep(2 * time.Second)
		}
	}
}
