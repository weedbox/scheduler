package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

const (
	scheduleTypeInterval              = "interval"
	scheduleTypeOnce                  = "once"
	scheduleTypeStartAtInterval       = "start_at_interval"
	scheduleTypeCron                  = "cron"
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

// CronSchedule runs a job based on a cron expression.
// Supports standard cron format: minute hour day month weekday
// Examples:
//   - "0 10 * * 5" - Every Friday at 10:00 AM
//   - "30 14 * * *" - Every day at 2:30 PM
//   - "0 0 1 * *" - First day of every month at midnight
//   - "*/5 * * * *" - Every 5 minutes
type CronSchedule struct {
	expression string
	schedule   cron.Schedule
}

// CronSpec defines a cron schedule using structured fields instead of a string expression.
// This provides a more type-safe and intuitive way to define cron schedules.
//
// Field values can be:
//   - Specific number: e.g., "5" for day of week (Friday)
//   - Wildcard: "*" for any value
//   - Range: "1-5" for Monday to Friday
//   - List: "1,3,5" for Monday, Wednesday, Friday
//   - Step: "*/5" for every 5 units
//
// Examples:
//   - Every Friday at 10:00 AM: {Minute: "0", Hour: "10", DayOfWeek: "5"}
//   - Every day at 2:30 PM: {Minute: "30", Hour: "14"}
//   - First day of month at midnight: {Minute: "0", Hour: "0", DayOfMonth: "1"}
//   - Every 5 minutes: {Minute: "*/5"}
type CronSpec struct {
	// Minute (0-59), default "*" (every minute)
	Minute string

	// Hour (0-23), default "*" (every hour)
	Hour string

	// DayOfMonth (1-31), default "*" (every day)
	DayOfMonth string

	// Month (1-12), default "*" (every month)
	Month string

	// DayOfWeek (0-6, Sunday=0), default "*" (every day)
	DayOfWeek string
}

// NewCronSchedule creates a new CronSchedule from a cron expression.
// The expression follows standard cron format: minute hour day month weekday
func NewCronSchedule(expression string) (*CronSchedule, error) {
	if expression == "" {
		return nil, ErrInvalidScheduleConfig
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(expression)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}

	return &CronSchedule{
		expression: expression,
		schedule:   schedule,
	}, nil
}

// NewCronScheduleFromSpec creates a new CronSchedule from a CronSpec.
// This provides a more structured way to define cron schedules without using string expressions.
// Empty fields default to "*" (any value).
//
// Examples:
//   - Every Friday at 10:00 AM:
//     NewCronScheduleFromSpec(&CronSpec{Minute: "0", Hour: "10", DayOfWeek: "5"})
//   - Every day at 2:30 PM:
//     NewCronScheduleFromSpec(&CronSpec{Minute: "30", Hour: "14"})
//   - Every 5 minutes:
//     NewCronScheduleFromSpec(&CronSpec{Minute: "*/5"})
func NewCronScheduleFromSpec(spec *CronSpec) (*CronSchedule, error) {
	if spec == nil {
		return nil, ErrInvalidScheduleConfig
	}

	// Apply defaults
	minute := spec.Minute
	if minute == "" {
		minute = "*"
	}

	hour := spec.Hour
	if hour == "" {
		hour = "*"
	}

	dayOfMonth := spec.DayOfMonth
	if dayOfMonth == "" {
		dayOfMonth = "*"
	}

	month := spec.Month
	if month == "" {
		month = "*"
	}

	dayOfWeek := spec.DayOfWeek
	if dayOfWeek == "" {
		dayOfWeek = "*"
	}

	// Build cron expression: minute hour day month weekday
	expression := fmt.Sprintf("%s %s %s %s %s", minute, hour, dayOfMonth, month, dayOfWeek)

	// Use NewCronSchedule to parse and validate
	return NewCronSchedule(expression)
}

// Next returns the next run time based on the cron expression.
func (s *CronSchedule) Next(t time.Time) time.Time {
	return s.schedule.Next(t)
}

// Expression returns the cron expression string.
func (s *CronSchedule) Expression() string {
	return s.expression
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
	case *CronSchedule:
		return scheduleTypeCron, s.Expression(), nil
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
	case scheduleTypeCron:
		if scheduleConfig == "" {
			return nil, ErrInvalidScheduleConfig
		}
		return NewCronSchedule(scheduleConfig)
	default:
		return nil, fmt.Errorf("unsupported schedule type %s", scheduleType)
	}
}

func farFutureTime(from time.Time) time.Time {
	loc := from.Location()
	return time.Date(from.Year()+int(farFutureYears), time.January, 1, 0, 0, 0, 0, loc)
}
