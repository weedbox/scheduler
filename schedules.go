package scheduler

import (
	"fmt"
	"strings"
	"time"
)

const (
	scheduleTypeInterval              = "interval"
	scheduleTypeOnce                  = "once"
	scheduleTypeStartAtInterval       = "start_at_interval"
	farFutureYears              int64 = 100
)

// IntervalSchedule runs a job at fixed intervals.
type IntervalSchedule struct {
	interval time.Duration
}

// NewIntervalSchedule creates a new IntervalSchedule.
func NewIntervalSchedule(interval time.Duration) (*IntervalSchedule, error) {
	if interval <= 0 {
		return nil, ErrInvalidInterval
	}
	return &IntervalSchedule{interval: interval}, nil
}

// Next returns the next run time based on the provided time.
func (s *IntervalSchedule) Next(t time.Time) time.Time {
	return t.Add(s.interval)
}

// Interval returns the configured interval duration.
func (s *IntervalSchedule) Interval() time.Duration {
	return s.interval
}

// OnceSchedule runs a job only once at a specific time.
type OnceSchedule struct {
	runTime time.Time
}

// NewOnceSchedule creates a new OnceSchedule.
func NewOnceSchedule(runTime time.Time) (*OnceSchedule, error) {
	if runTime.IsZero() {
		return nil, ErrInvalidScheduleTime
	}
	return &OnceSchedule{runTime: runTime}, nil
}

// Next returns the scheduled run time if it's still in the future, otherwise a far future time.
func (s *OnceSchedule) Next(t time.Time) time.Time {
	if t.Before(s.runTime) {
		return s.runTime
	}
	return farFutureTime(t)
}

// RunAt returns the configured run time.
func (s *OnceSchedule) RunAt() time.Time {
	return s.runTime
}

// StartAtIntervalSchedule runs a job at a fixed interval starting from a specific time.
type StartAtIntervalSchedule struct {
	startAt  time.Time
	interval time.Duration
}

// NewStartAtIntervalSchedule creates a new StartAtIntervalSchedule.
func NewStartAtIntervalSchedule(startAt time.Time, interval time.Duration) (*StartAtIntervalSchedule, error) {
	if interval <= 0 {
		return nil, ErrInvalidInterval
	}
	if startAt.IsZero() {
		return nil, ErrInvalidScheduleTime
	}
	return &StartAtIntervalSchedule{
		startAt:  startAt,
		interval: interval,
	}, nil
}

// Next returns the next run time based on the configured start time and interval.
func (s *StartAtIntervalSchedule) Next(t time.Time) time.Time {
	if t.Before(s.startAt) {
		return s.startAt
	}

	elapsed := t.Sub(s.startAt)
	cycles := elapsed / s.interval
	if elapsed%s.interval == 0 {
		return s.startAt.Add((cycles + 1) * s.interval)
	}

	return s.startAt.Add((cycles + 1) * s.interval)
}

// StartAt returns the configured start time.
func (s *StartAtIntervalSchedule) StartAt() time.Time {
	return s.startAt
}

// Interval returns the configured interval.
func (s *StartAtIntervalSchedule) Interval() time.Duration {
	return s.interval
}

// BasicScheduleCodec provides a schedule codec for the built-in schedules.
type BasicScheduleCodec struct{}

// NewBasicScheduleCodec creates a new BasicScheduleCodec instance.
func NewBasicScheduleCodec() *BasicScheduleCodec {
	return &BasicScheduleCodec{}
}

// Encode implements ScheduleCodec.Encode.
func (c *BasicScheduleCodec) Encode(schedule Schedule) (string, string, error) {
	switch s := schedule.(type) {
	case *IntervalSchedule:
		return scheduleTypeInterval, s.Interval().String(), nil
	case *OnceSchedule:
		return scheduleTypeOnce, s.RunAt().UTC().Format(time.RFC3339Nano), nil
	case *StartAtIntervalSchedule:
		config := fmt.Sprintf("%s|%s", s.StartAt().UTC().Format(time.RFC3339Nano), s.Interval().String())
		return scheduleTypeStartAtInterval, config, nil
	default:
		return "", "", fmt.Errorf("unsupported schedule type %T", schedule)
	}
}

// Decode implements ScheduleCodec.Decode.
func (c *BasicScheduleCodec) Decode(scheduleType string, scheduleConfig string) (Schedule, error) {
	switch scheduleType {
	case scheduleTypeInterval:
		if scheduleConfig == "" {
			return nil, ErrInvalidScheduleConfig
		}
		d, err := time.ParseDuration(scheduleConfig)
		if err != nil {
			return nil, err
		}
		return NewIntervalSchedule(d)
	case scheduleTypeOnce:
		if scheduleConfig == "" {
			return nil, ErrInvalidScheduleConfig
		}
		runAt, err := time.Parse(time.RFC3339Nano, scheduleConfig)
		if err != nil {
			return nil, err
		}
		return NewOnceSchedule(runAt)
	case scheduleTypeStartAtInterval:
		parts := strings.Split(scheduleConfig, "|")
		if len(parts) != 2 {
			return nil, ErrInvalidScheduleConfig
		}
		startAt, err := time.Parse(time.RFC3339Nano, parts[0])
		if err != nil {
			return nil, err
		}
		interval, err := time.ParseDuration(parts[1])
		if err != nil {
			return nil, err
		}
		return NewStartAtIntervalSchedule(startAt, interval)
	default:
		return nil, fmt.Errorf("unsupported schedule type %s", scheduleType)
	}
}

func farFutureTime(from time.Time) time.Time {
	loc := from.Location()
	return time.Date(from.Year()+int(farFutureYears), time.January, 1, 0, 0, 0, 0, loc)
}
