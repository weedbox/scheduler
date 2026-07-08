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
	scheduleTypeDaily                 = "daily"
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

	cycles := t.Sub(s.startAt) / s.interval
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

// DailySchedule runs a job every day at a fixed wall-clock time, evaluated
// in the location of the reference time passed to Next (typically the
// scheduler host's local time zone).
type DailySchedule struct {
	hour   int
	minute int
	second int
}

// NewDailySchedule creates a schedule that fires once per day at the given
// wall-clock time.
func NewDailySchedule(hour, minute, second int) (*DailySchedule, error) {
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return nil, ErrInvalidScheduleTime
	}
	return &DailySchedule{hour: hour, minute: minute, second: second}, nil
}

// Next returns the next occurrence of the configured wall-clock time strictly
// after t.
func (s *DailySchedule) Next(t time.Time) time.Time {
	next := time.Date(t.Year(), t.Month(), t.Day(), s.hour, s.minute, s.second, 0, t.Location())
	if !next.After(t) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// At returns the configured hour, minute, and second.
func (s *DailySchedule) At() (hour, minute, second int) {
	return s.hour, s.minute, s.second
}

// CronSchedule runs a job based on a cron expression.
// Supports standard cron format: minute hour day month weekday
//
// Times are evaluated in the location of the reference time passed to Next —
// in practice the scheduler host's local time zone. A cluster whose nodes run
// in different time zones will disagree on when "0 10 * * *" fires; pin the
// hosts (or container TZ) to a common zone if that matters.
//
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
	case *DailySchedule:
		hour, minute, second := s.At()
		return scheduleTypeDaily, fmt.Sprintf("%02d:%02d:%02d", hour, minute, second), nil
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
	case scheduleTypeDaily:
		var hour, minute, second int
		if _, err := fmt.Sscanf(scheduleConfig, "%d:%d:%d", &hour, &minute, &second); err != nil {
			return nil, ErrInvalidScheduleConfig
		}
		return NewDailySchedule(hour, minute, second)
	default:
		return nil, fmt.Errorf("unsupported schedule type %s", scheduleType)
	}
}

func farFutureTime(from time.Time) time.Time {
	loc := from.Location()
	return time.Date(from.Year()+int(farFutureYears), time.January, 1, 0, 0, 0, 0, loc)
}

// scheduleBuilder is the built-in ScheduleBuilder implementation.
type scheduleBuilder struct{}

// NewScheduleBuilder returns a ScheduleBuilder that creates the built-in
// schedule types, all of which round-trip through BasicScheduleCodec.
func NewScheduleBuilder() ScheduleBuilder {
	return &scheduleBuilder{}
}

// Every creates a schedule that runs at fixed intervals.
func (b *scheduleBuilder) Every(duration time.Duration) (Schedule, error) {
	return NewIntervalSchedule(duration)
}

// Cron creates a schedule from a cron expression.
func (b *scheduleBuilder) Cron(expression string) (Schedule, error) {
	return NewCronSchedule(expression)
}

// At creates a schedule that runs daily at the given wall-clock time.
func (b *scheduleBuilder) At(hour, minute, second int) (Schedule, error) {
	return NewDailySchedule(hour, minute, second)
}

// Once creates a schedule that runs only once at the specified time.
func (b *scheduleBuilder) Once(t time.Time) (Schedule, error) {
	return NewOnceSchedule(t)
}
