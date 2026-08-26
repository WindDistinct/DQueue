package main

import "testing"

func TestEnqueueDequeue(t *testing.T) {
	queue := &Queue{}

	queue.Enqueue(NewJob("test", "payload"))

	job, err := queue.Dequeue()

	if err != nil {
		t.Fatalf("no se esperaba error: %v", err)
	}

	if job.Name != "test" {
		t.Fatalf("esperaba Name=%q, obtuve %q", "test", job.Name)
	}
}

func TestDequeueEmpty(t *testing.T) {
	queue := &Queue{}

	_, err := queue.Dequeue()

	if err == nil {
		t.Fatalf("se esperaba error, no hubo")
	}
}
