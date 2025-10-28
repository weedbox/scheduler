# Cron Schedule Example

This example demonstrates how to use cron expressions to schedule jobs at specific times.

## Running the Example

```bash
go run main.go
```

## What This Example Does

The example creates three jobs with different cron schedules:

1. **Weekly Report** - Runs every Friday at 10:00 AM
   - Cron expression: `0 10 * * 5`

2. **Daily Backup** - Runs every day at 2:30 PM
   - Cron expression: `30 14 * * *`

3. **5-Minute Task** - Runs every 5 minutes
   - Cron expression: `*/5 * * * *`

## Cron Expression Format

The cron expressions follow the standard 5-field format:

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of week (0 - 6) (Sunday to Saturday)
│ │ │ │ │
* * * * *
```

## Common Cron Examples

- `0 10 * * 5` - Every Friday at 10:00 AM
- `30 14 * * *` - Every day at 2:30 PM
- `0 0 1 * *` - First day of every month at midnight
- `*/5 * * * *` - Every 5 minutes
- `0 9 * * 1-5` - Monday to Friday at 9:00 AM
- `0 */2 * * *` - Every 2 hours
- `15 10 * * *` - Every day at 10:15 AM

## Special Characters

- `*` - Any value
- `,` - Value list separator (e.g., `1,3,5`)
- `-` - Range of values (e.g., `1-5`)
- `/` - Step values (e.g., `*/5` means every 5 units)
