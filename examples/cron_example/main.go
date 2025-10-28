package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Weedbox/scheduler"
)

func main() {
	// Create scheduler with in-memory storage
	storage := scheduler.NewMemoryStorage()
	handler := func(ctx context.Context, event scheduler.JobEvent) error {
		switch event.ID() {
		case "friday-report":
			fmt.Printf("[%s] Running weekly report (Every Friday at 10:00 AM)\n", time.Now().Format("2006-01-02 15:04:05"))
		case "daily-backup":
			fmt.Printf("[%s] Running daily backup (Every day at 2:30 PM)\n", time.Now().Format("2006-01-02 15:04:05"))
		case "every-5min":
			fmt.Printf("[%s] Running 5-minute task\n", time.Now().Format("2006-01-02 15:04:05"))
		}
		return nil
	}

	sched := scheduler.NewScheduler(storage, handler, scheduler.NewBasicScheduleCodec())

	// Start the scheduler
	ctx := context.Background()
	if err := sched.Start(ctx); err != nil {
		panic(err)
	}
	if err := sched.WaitUntilRunning(ctx); err != nil {
		panic(err)
	}
	defer sched.Stop(ctx)

	// Example 1: Every Friday at 10:00 AM
	fridaySchedule, err := scheduler.NewCronSchedule("0 10 * * 5")
	if err != nil {
		panic(err)
	}
	if err := sched.AddJob("friday-report", fridaySchedule, map[string]string{"type": "weekly"}); err != nil {
		panic(err)
	}
	fmt.Println("✓ Added job: Weekly report (Every Friday at 10:00 AM)")

	// Example 2: Every day at 2:30 PM
	dailySchedule, err := scheduler.NewCronSchedule("30 14 * * *")
	if err != nil {
		panic(err)
	}
	if err := sched.AddJob("daily-backup", dailySchedule, map[string]string{"type": "daily"}); err != nil {
		panic(err)
	}
	fmt.Println("✓ Added job: Daily backup (Every day at 2:30 PM)")

	// Example 3: Every 5 minutes (for demonstration)
	fiveMinSchedule, err := scheduler.NewCronSchedule("*/5 * * * *")
	if err != nil {
		panic(err)
	}
	if err := sched.AddJob("every-5min", fiveMinSchedule, map[string]string{"type": "frequent"}); err != nil {
		panic(err)
	}
	fmt.Println("✓ Added job: Every 5 minutes task")

	// Display next run times
	fmt.Println("\n📅 Next scheduled runs:")
	for _, job := range sched.ListJobs() {
		fmt.Printf("  - %s: %s\n", job.ID(), job.NextRun().Format("2006-01-02 15:04:05 Mon"))
	}

	// Wait for interrupt signal
	fmt.Println("\n⏳ Scheduler is running. Press Ctrl+C to stop...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Shutting down gracefully...")
}
