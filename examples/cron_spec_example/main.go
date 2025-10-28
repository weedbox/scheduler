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
			fmt.Printf("[%s] 📊 Weekly Report - Every Friday at 10:00 AM\n",
				time.Now().Format("2006-01-02 15:04:05"))
		case "daily-backup":
			fmt.Printf("[%s] 💾 Daily Backup - Every day at 2:30 PM\n",
				time.Now().Format("2006-01-02 15:04:05"))
		case "weekday-task":
			fmt.Printf("[%s] 💼 Weekday Task - Monday to Friday at 9:00 AM\n",
				time.Now().Format("2006-01-02 15:04:05"))
		case "every-5min":
			fmt.Printf("[%s] ⏰ Quick Task - Every 5 minutes\n",
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

	fmt.Println("✨ Using CronSpec - Structured API for Cron Schedules")
	fmt.Println("=" + string(make([]byte, 50)) + "=")
	fmt.Println()

	// Example 1: Every Friday at 10:00 AM
	fridaySpec := &scheduler.CronSpec{
		Minute:    "0",
		Hour:      "10",
		DayOfWeek: "5", // Friday (0=Sunday, 5=Friday)
	}
	fridaySchedule, err := scheduler.NewCronScheduleFromSpec(fridaySpec)
	if err != nil {
		panic(err)
	}
	if err := sched.AddJob("friday-report", fridaySchedule, map[string]string{"type": "weekly"}); err != nil {
		panic(err)
	}
	fmt.Println("✓ Added: Weekly report - Every Friday at 10:00 AM")
	fmt.Printf("  CronSpec: {Minute: %q, Hour: %q, DayOfWeek: %q}\n",
		fridaySpec.Minute, fridaySpec.Hour, fridaySpec.DayOfWeek)

	// Example 2: Every day at 2:30 PM
	dailySpec := &scheduler.CronSpec{
		Minute: "30",
		Hour:   "14",
	}
	dailySchedule, err := scheduler.NewCronScheduleFromSpec(dailySpec)
	if err != nil {
		panic(err)
	}
	if err := sched.AddJob("daily-backup", dailySchedule, map[string]string{"type": "daily"}); err != nil {
		panic(err)
	}
	fmt.Println("✓ Added: Daily backup - Every day at 2:30 PM")
	fmt.Printf("  CronSpec: {Minute: %q, Hour: %q}\n", dailySpec.Minute, dailySpec.Hour)

	// Example 3: Monday to Friday at 9:00 AM
	weekdaySpec := &scheduler.CronSpec{
		Minute:    "0",
		Hour:      "9",
		DayOfWeek: "1-5", // Monday to Friday
	}
	weekdaySchedule, err := scheduler.NewCronScheduleFromSpec(weekdaySpec)
	if err != nil {
		panic(err)
	}
	if err := sched.AddJob("weekday-task", weekdaySchedule, map[string]string{"type": "weekday"}); err != nil {
		panic(err)
	}
	fmt.Println("✓ Added: Weekday task - Monday to Friday at 9:00 AM")
	fmt.Printf("  CronSpec: {Minute: %q, Hour: %q, DayOfWeek: %q}\n",
		weekdaySpec.Minute, weekdaySpec.Hour, weekdaySpec.DayOfWeek)

	// Example 4: Every 5 minutes
	frequentSpec := &scheduler.CronSpec{
		Minute: "*/5",
	}
	frequentSchedule, err := scheduler.NewCronScheduleFromSpec(frequentSpec)
	if err != nil {
		panic(err)
	}
	if err := sched.AddJob("every-5min", frequentSchedule, map[string]string{"type": "frequent"}); err != nil {
		panic(err)
	}
	fmt.Println("✓ Added: Quick task - Every 5 minutes (for demo)")
	fmt.Printf("  CronSpec: {Minute: %q}\n", frequentSpec.Minute)

	// Display next run times
	fmt.Println()
	fmt.Println("📅 Next scheduled runs:")
	for _, job := range sched.ListJobs() {
		fmt.Printf("  - %-15s: %s\n", job.ID(), job.NextRun().Format("2006-01-02 15:04:05 Mon"))
	}

	fmt.Println()
	fmt.Println("💡 Benefits of CronSpec:")
	fmt.Println("  • Type-safe field access")
	fmt.Println("  • No need to remember cron expression syntax")
	fmt.Println("  • IDE autocomplete support")
	fmt.Println("  • Clear and self-documenting code")

	// Wait for interrupt signal
	fmt.Println()
	fmt.Println("⏳ Scheduler is running. Press Ctrl+C to stop...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println()
	fmt.Println("🛑 Shutting down gracefully...")
}
