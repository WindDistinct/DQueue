package main

import (
	"fmt"
	"time"
)

type Job struct {
	ID        int
	Name      string
	Payload   string
	CreatedAt time.Time
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

func main() {
	fmt.Println("DQueue starting...")

	var createdJobs []*Job

	createdJobs = append(createdJobs, NewJob("test 1", "payload 1"))
	createdJobs = append(createdJobs, NewJob("test 2", "payload 2"))
	createdJobs = append(createdJobs, NewJob("test 3", "payload 3"))

	for i, job := range createdJobs {
		fmt.Printf("Job Nro.%d queued: %s, %s\n", i, job.Name, job.Payload)
	}

}
