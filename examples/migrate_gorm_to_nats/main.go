package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Weedbox/scheduler"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	ctx := context.Background()

	// -------------------------------------------------------
	// Step 1: Set up the GORM source storage (old backend)
	// -------------------------------------------------------
	dbPath := "scheduler.db"
	if p := os.Getenv("DB_PATH"); p != "" {
		dbPath = p
	}

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	gormStorage := scheduler.NewGormStorage(db)
	if err := gormStorage.Initialize(ctx); err != nil {
		log.Fatalf("Failed to initialize GORM storage: %v", err)
	}
	defer gormStorage.Close(ctx)

	// Seed some demo data if the DB is empty
	seedDemoData(ctx, gormStorage)

	// -------------------------------------------------------
	// Step 2: Set up the NATS destination storage (new backend)
	// -------------------------------------------------------
	natsURL := nats.DefaultURL
	if url := os.Getenv("NATS_URL"); url != "" {
		natsURL = url
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	// Use default bucket names — they match NewNATSScheduler defaults:
	//   SCHEDULER_JOBS, SCHEDULER_EXECUTIONS
	natsStorage := scheduler.NewNATSStorage(js)
	if err := natsStorage.Initialize(ctx); err != nil {
		log.Fatalf("Failed to initialize NATS storage: %v", err)
	}
	defer natsStorage.Close(ctx)

	// -------------------------------------------------------
	// Step 3: Migrate!
	// -------------------------------------------------------
	fmt.Println("Starting migration from GORM to NATS JetStream...")
	result, err := scheduler.MigrateStorage(ctx, gormStorage, natsStorage)
	if err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	fmt.Printf("Migration complete:\n")
	fmt.Printf("  Jobs migrated:       %d\n", result.JobsMigrated)
	fmt.Printf("  Jobs skipped:        %d\n", result.JobsSkipped)
	fmt.Printf("  Executions migrated: %d\n", result.ExecutionsMigrated)
	fmt.Printf("  Executions skipped:  %d\n", result.ExecutionsSkipped)

	// -------------------------------------------------------
	// Step 4: Verify — list jobs in NATS storage
	// -------------------------------------------------------
	fmt.Println("\nJobs now in NATS KV:")
	jobs, _ := natsStorage.ListJobs(ctx)
	for _, job := range jobs {
		fmt.Printf("  - %s (type: %s, status: %s, next_run: %s)\n",
			job.ID, job.ScheduleType, job.Status,
			job.NextRun.Format("2006-01-02 15:04:05"),
		)
	}

	// -------------------------------------------------------
	// Step 5: Start the NATS scheduler — jobs resume automatically
	// -------------------------------------------------------
	fmt.Println("\nStarting NATS scheduler — jobs will resume from KV data...")

	handler := func(ctx context.Context, event scheduler.JobEvent) error {
		fmt.Printf("[%s] Job executed: %s (metadata: %v)\n",
			time.Now().Format("15:04:05"),
			event.ID(),
			event.Metadata(),
		)
		return nil
	}

	sched := scheduler.NewNATSScheduler(js, handler)
	if err := sched.Start(ctx); err != nil {
		log.Fatalf("Failed to start NATS scheduler: %v", err)
	}
	defer sched.Stop(ctx)

	fmt.Printf("Scheduler running with %d restored job(s). Waiting 15 seconds...\n", len(sched.ListJobs()))

	time.Sleep(15 * time.Second)
	fmt.Println("Done.")
}

// seedDemoData creates sample jobs and executions in GORM if the storage is empty.
func seedDemoData(ctx context.Context, storage *scheduler.GormStorage) {
	jobs, _ := storage.ListJobs(ctx)
	if len(jobs) > 0 {
		fmt.Printf("GORM storage already has %d job(s), skipping seed.\n", len(jobs))
		return
	}

	fmt.Println("Seeding demo data into GORM storage...")
	now := time.Now()

	// Job 1: interval schedule
	job1 := &scheduler.JobData{
		ID:             "heartbeat",
		ScheduleType:   "interval",
		ScheduleConfig: `{"interval":"5s"}`,
		Status:         scheduler.JobStatusPending,
		NextRun:        now.Add(5 * time.Second),
		CreatedAt:      now,
		UpdatedAt:      now,
		Metadata:       map[string]string{"type": "interval", "desc": "every 5s"},
	}
	if err := storage.SaveJob(ctx, job1); err != nil {
		log.Fatalf("Failed to save job1: %v", err)
	}

	// Job 2: cron schedule
	job2 := &scheduler.JobData{
		ID:             "daily-report",
		ScheduleType:   "cron",
		ScheduleConfig: `{"expression":"0 9 * * *"}`,
		Status:         scheduler.JobStatusPending,
		NextRun:        now.Add(time.Hour),
		CreatedAt:      now,
		UpdatedAt:      now,
		Metadata:       map[string]string{"type": "cron", "desc": "daily at 9am"},
	}
	if err := storage.SaveJob(ctx, job2); err != nil {
		log.Fatalf("Failed to save job2: %v", err)
	}

	// Add some execution records for job1
	for i := 0; i < 3; i++ {
		exec := &scheduler.ExecutionRecord{
			JobID:       "heartbeat",
			ExecutionID: fmt.Sprintf("exec-%d", i+1),
			StartTime:   now.Add(-time.Duration(3-i) * 10 * time.Second),
			EndTime:     now.Add(-time.Duration(3-i)*10*time.Second + time.Second),
			Duration:    time.Second,
			Status:      scheduler.JobStatusCompleted,
		}
		if err := storage.SaveExecution(ctx, exec); err != nil {
			log.Fatalf("Failed to save execution: %v", err)
		}
	}

	fmt.Println("  Seeded 2 jobs and 3 execution records.")
}
