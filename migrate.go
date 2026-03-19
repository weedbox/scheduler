package scheduler

import (
	"context"
	"errors"
	"fmt"
)

// MigrateResult contains the results of a storage migration.
type MigrateResult struct {
	JobsMigrated       int
	JobsSkipped        int
	ExecutionsMigrated int
	ExecutionsSkipped  int
}

// MigrateStorage copies all jobs and execution records from src to dst.
// It is idempotent: jobs that already exist in dst are skipped.
// This function works with any Storage implementations (e.g. GORM→NATS, Memory→GORM, etc).
func MigrateStorage(ctx context.Context, src Storage, dst Storage) (*MigrateResult, error) {
	result := &MigrateResult{}

	jobs, err := src.ListJobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs from source: %w", err)
	}

	for _, job := range jobs {
		if err := dst.SaveJob(ctx, job); err != nil {
			if errors.Is(err, ErrJobDataAlreadyExists) {
				result.JobsSkipped++
			} else {
				return result, fmt.Errorf("failed to save job %s: %w", job.ID, err)
			}
		} else {
			result.JobsMigrated++
		}

		executions, err := src.ListExecutions(ctx, job.ID, nil)
		if err != nil {
			return result, fmt.Errorf("failed to list executions for job %s: %w", job.ID, err)
		}

		for _, exec := range executions {
			existing, _ := dst.GetExecution(ctx, exec.ExecutionID)
			if existing != nil {
				result.ExecutionsSkipped++
				continue
			}

			if err := dst.SaveExecution(ctx, exec); err != nil {
				return result, fmt.Errorf("failed to save execution %s: %w", exec.ExecutionID, err)
			}
			result.ExecutionsMigrated++
		}
	}

	return result, nil
}
