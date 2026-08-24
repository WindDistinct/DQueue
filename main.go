package main

import (
	"errors"
	"fmt"
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
	q.jobs = append(q.jobs, job)
}

func (q *Queue) Dequeue() (job *Job, err error) {
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

	queue := &Queue{}

	queue.Enqueue(NewJob("test 1", "payload 1"))
	queue.Enqueue(NewJob("test 2", "payload 2"))
	queue.Enqueue(NewJob("test 3", "payload 3"))

	for i := 0; i < 4; i++ {
		job, err := queue.Dequeue()
		if err != nil {
			fmt.Printf("%d: No hay más Jobs en la cola\n", i+1)
		} else {
			fmt.Printf("Se retiro el Job %d de la cola\n", job.ID)
		}
	}

}
