package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Weedbox/scheduler"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// Connect to NATS server
	natsURL := nats.DefaultURL
	if url := os.Getenv("NATS_URL"); url != "" {
		natsURL = url
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	fmt.Printf("Connected to NATS at %s\n", natsURL)

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	// Define job handler
	handler := func(ctx context.Context, event scheduler.JobEvent) error {
		fmt.Printf("[%s] Job executed: %s (delay: %s, metadata: %v)\n",
			time.Now().Format("15:04:05"),
			event.ID(),
			event.Delay().Truncate(time.Millisecond),
			event.Metadata(),
		)
		return nil
	}

	// Create NATS scheduler
	sched := scheduler.NewNATSScheduler(js, handler,
		scheduler.WithNATSStreamName("EXAMPLE_SCHEDULER"),
		scheduler.WithNATSSubjectPrefix("example.scheduler"),
		scheduler.WithNATSConsumerName("example-worker"),
		scheduler.WithNATSSchedulerJobBucket("EXAMPLE_SCHEDULER_JOBS"),
		scheduler.WithNATSSchedulerExecBucket("EXAMPLE_SCHEDULER_EXECS"),
	)

	// Start the scheduler
	ctx := context.Background()
	if err := sched.Start(ctx); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}
	defer sched.Stop(ctx)

	// Check if jobs were restored from a previous run
	existingJobs := sched.ListJobs()
	if len(existingJobs) > 0 {
		fmt.Printf("\nRestored %d job(s) from NATS KV:\n", len(existingJobs))
		for _, job := range existingJobs {
			fmt.Printf("  - %s (next run: %s)\n",
				job.ID(),
				job.NextRun().Format("15:04:05"),
			)
		}
	} else {
		fmt.Println("\nNo existing jobs found. Creating new jobs...")

		// Example 1: Interval schedule - runs every 3 seconds
		intervalSchedule, err := scheduler.NewIntervalSchedule(3 * time.Second)
		if err != nil {
			log.Fatal(err)
		}
		if err := sched.AddJob("heartbeat", intervalSchedule, map[string]string{
			"type": "interval",
			"desc": "every 3s",
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("  + Added: heartbeat (every 3 seconds)")

		// Example 2: One-time schedule - runs once after 5 seconds
		onceSchedule, err := scheduler.NewOnceSchedule(time.Now().Add(5 * time.Second))
		if err != nil {
			log.Fatal(err)
		}
		if err := sched.AddJob("one-time-task", onceSchedule, map[string]string{
			"type": "once",
			"desc": "runs once after 5s",
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("  + Added: one-time-task (once after 5 seconds)")

		// Example 3: Cron schedule - runs every minute
		cronSchedule, err := scheduler.NewCronSchedule("* * * * *")
		if err != nil {
			log.Fatal(err)
		}
		if err := sched.AddJob("every-minute", cronSchedule, map[string]string{
			"type": "cron",
			"desc": "every minute",
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("  + Added: every-minute (cron: * * * * *)")

		// Example 4: StartAt interval schedule - starts after 10s, then every 5s
		startAtSchedule, err := scheduler.NewStartAtIntervalSchedule(
			time.Now().Add(10*time.Second),
			5*time.Second,
		)
		if err != nil {
			log.Fatal(err)
		}
		if err := sched.AddJob("delayed-interval", startAtSchedule, map[string]string{
			"type": "start_at_interval",
			"desc": "starts after 10s, then every 5s",
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("  + Added: delayed-interval (starts after 10s, then every 5s)")
	}

	// Display scheduled runs
	fmt.Println("\nNext scheduled runs:")
	for _, job := range sched.ListJobs() {
		fmt.Printf("  - %s: %s\n", job.ID(), job.NextRun().Format("15:04:05"))
	}

	fmt.Println("\nScheduler is running. Press Ctrl+C to stop...")
	fmt.Println("Jobs are persisted in NATS JetStream KV. Restart to see them restored.")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down gracefully...")
}
