package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Weedbox/scheduler"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Multi-instance scheduling on plain PostgreSQL — no NATS required.
//
// Run this program on several terminals at once; every instance points at
// the same database and due jobs are claimed with FOR UPDATE SKIP LOCKED,
// so each tick executes on exactly one instance. Kill an instance while a
// job runs and a surviving instance takes the job over once its lease
// expires.
//
//	POSTGRES_DSN="host=localhost port=5432 user=postgres password=test dbname=postgres sslmode=disable" \
//	  go run ./examples/postgres_multi_instance
func main() {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		fmt.Println("set POSTGRES_DSN to a PostgreSQL connection string")
		os.Exit(1)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	instance, _ := os.Hostname()
	instance = fmt.Sprintf("%s/%d", instance, os.Getpid())

	handler := func(ctx context.Context, event scheduler.JobEvent) error {
		fmt.Printf("[%s] ⏰ %s executed on instance %s\n",
			time.Now().Format("15:04:05"), event.ID(), instance)
		return nil
	}

	sched := scheduler.NewPostgresScheduler(db, handler, scheduler.NewBasicScheduleCodec(),
		scheduler.WithPostgresLeaseDuration(30*time.Second),
		scheduler.WithPostgresSchedulerLogger(func(msg string, kv ...any) {
			fmt.Println("scheduler:", msg, kv)
		}),
	)

	ctx := context.Background()
	if err := sched.Start(ctx); err != nil {
		panic(err)
	}
	defer sched.Stop(ctx)

	// Every instance tries to register the same job; whoever gets there
	// first wins and the rest see ErrJobAlreadyExists.
	schedule, _ := scheduler.NewIntervalSchedule(5 * time.Second)
	if err := sched.AddJob("shared-tick", schedule, map[string]string{"type": "demo"}); err != nil {
		if !errors.Is(err, scheduler.ErrJobAlreadyExists) {
			panic(err)
		}
		fmt.Println("job already registered by another instance")
	}

	fmt.Printf("instance %s running — press Ctrl+C to stop\n", instance)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
