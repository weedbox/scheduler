package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Weedbox/scheduler"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	// Create or open database
	db, err := gorm.Open(sqlite.Open("scheduler.db"), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// Create scheduler with GORM storage for persistence
	storage := scheduler.NewGormStorage(db)
	handler := func(ctx context.Context, event scheduler.JobEvent) error {
		switch event.ID() {
		case "friday-report":
			fmt.Printf("[%s] 📊 Running weekly report (Every Friday at 10:00 AM)\n",
				time.Now().Format("2006-01-02 15:04:05"))
		case "daily-backup":
			fmt.Printf("[%s] 💾 Running daily backup (Every day at 2:30 PM)\n",
				time.Now().Format("2006-01-02 15:04:05"))
		case "every-minute":
			fmt.Printf("[%s] ⏰ Running minute task (for demo)\n",
				time.Now().Format("2006-01-02 15:04:05"))
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

	// Check if jobs already exist in database (from previous run)
	existingJobs := sched.ListJobs()
	if len(existingJobs) > 0 {
		fmt.Printf("\n✅ Restored %d job(s) from database:\n", len(existingJobs))
		for _, job := range existingJobs {
			fmt.Printf("  - %s (Next run: %s)\n",
				job.ID(),
				job.NextRun().Format("2006-01-02 15:04:05 Mon"))
		}
	} else {
		fmt.Println("\n📝 No existing jobs found. Creating new jobs...")

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

		// Example 3: Every minute (for demonstration)
		everyMinuteSchedule, err := scheduler.NewCronSchedule("* * * * *")
		if err != nil {
			panic(err)
		}
		if err := sched.AddJob("every-minute", everyMinuteSchedule, map[string]string{"type": "frequent"}); err != nil {
			panic(err)
		}
		fmt.Println("✓ Added job: Every minute task (for demo)")

		// Display next run times
		fmt.Println("\n📅 Next scheduled runs:")
		for _, job := range sched.ListJobs() {
			fmt.Printf("  - %s: %s\n", job.ID(), job.NextRun().Format("2006-01-02 15:04:05 Mon"))
		}
	}

	fmt.Println("\n💡 Jobs are persisted to 'scheduler.db'")
	fmt.Println("   You can restart this program and jobs will continue from where they left off!")
	fmt.Println("\n⏳ Scheduler is running. Press Ctrl+C to stop...")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Shutting down gracefully...")
	fmt.Println("   Jobs will be preserved and will continue after restart.")
}
