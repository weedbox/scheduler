# CronSpec Example - Structured API for Cron Schedules

This example demonstrates how to use `CronSpec` to create cron schedules using structured fields instead of string expressions.

## Why Use CronSpec?

Instead of remembering cron expression syntax:
```go
// Traditional way - need to remember the syntax
schedule, _ := scheduler.NewCronSchedule("0 10 * * 5")
```

Use structured fields:
```go
// CronSpec way - clear and self-documenting
spec := &scheduler.CronSpec{
    Minute:    "0",
    Hour:      "10",
    DayOfWeek: "5",  // Friday
}
schedule, _ := scheduler.NewCronScheduleFromSpec(spec)
```

## Benefits

✅ **Type-Safe** - Each field is clearly defined
✅ **IDE Support** - Autocomplete and inline documentation
✅ **Self-Documenting** - Easy to understand at a glance
✅ **No Syntax Errors** - Harder to make mistakes with field names
✅ **Same Power** - Supports all cron features (ranges, lists, steps)

## Running the Example

```bash
go run main.go
```

## CronSpec Fields

```go
type CronSpec struct {
    Minute     string  // 0-59, default "*"
    Hour       string  // 0-23, default "*"
    DayOfMonth string  // 1-31, default "*"
    Month      string  // 1-12, default "*"
    DayOfWeek  string  // 0-6 (Sunday=0), default "*"
}
```

## Examples

### Every Friday at 10:00 AM
```go
spec := &scheduler.CronSpec{
    Minute:    "0",
    Hour:      "10",
    DayOfWeek: "5",
}
```

### Every Day at 2:30 PM
```go
spec := &scheduler.CronSpec{
    Minute: "30",
    Hour:   "14",
}
```

### Monday to Friday at 9:00 AM
```go
spec := &scheduler.CronSpec{
    Minute:    "0",
    Hour:      "9",
    DayOfWeek: "1-5",  // Range: Monday to Friday
}
```

### Every 5 Minutes
```go
spec := &scheduler.CronSpec{
    Minute: "*/5",  // Step value
}
```

### First Day of Every Month at Midnight
```go
spec := &scheduler.CronSpec{
    Minute:     "0",
    Hour:       "0",
    DayOfMonth: "1",
}
```

### Multiple Days (Mon, Wed, Fri)
```go
spec := &scheduler.CronSpec{
    Minute:    "0",
    Hour:      "9",
    DayOfWeek: "1,3,5",  // List: Monday, Wednesday, Friday
}
```

## Field Value Formats

Each field supports:
- **Specific value**: `"5"` - exactly 5
- **Wildcard**: `"*"` - every value
- **Range**: `"1-5"` - values 1 through 5
- **List**: `"1,3,5"` - values 1, 3, and 5
- **Step**: `"*/5"` - every 5 units
- **Combined**: `"1-5,10,*/15"` - complex patterns

## DayOfWeek Values

- `0` or `7` = Sunday
- `1` = Monday
- `2` = Tuesday
- `3` = Wednesday
- `4` = Thursday
- `5` = Friday
- `6` = Saturday

## Comparison

### Traditional Cron Expression
```go
schedule, _ := scheduler.NewCronSchedule("0 10 * * 5")
// What does "5" mean? Need to remember: Friday
```

### CronSpec
```go
spec := &scheduler.CronSpec{
    Minute:    "0",
    Hour:      "10",
    DayOfWeek: "5",  // Comment clearly shows it's Friday
}
schedule, _ := scheduler.NewCronScheduleFromSpec(spec)
// Much clearer and self-documenting!
```

## Generated Expression

CronSpec automatically generates the correct cron expression:

```go
spec := &scheduler.CronSpec{
    Minute: "30",
    Hour:   "14",
}
schedule, _ := scheduler.NewCronScheduleFromSpec(spec)

// Behind the scenes, this creates: "30 14 * * *"
fmt.Println(schedule.Expression()) // Output: "30 14 * * *"
```

## Best Practices

1. **Use Comments** - Explain what the schedule means:
   ```go
   spec := &scheduler.CronSpec{
       Minute:    "0",
       Hour:      "10",
       DayOfWeek: "5",  // Every Friday at 10:00 AM
   }
   ```

2. **Define Constants** - For frequently used values:
   ```go
   const (
       Friday    = "5"
       Monday    = "1"
       Weekdays  = "1-5"
   )

   spec := &scheduler.CronSpec{
       Minute:    "0",
       Hour:      "9",
       DayOfWeek: Weekdays,
   }
   ```

3. **Validate Early** - Check errors immediately:
   ```go
   schedule, err := scheduler.NewCronScheduleFromSpec(spec)
   if err != nil {
       log.Fatalf("Invalid cron spec: %v", err)
   }
   ```
