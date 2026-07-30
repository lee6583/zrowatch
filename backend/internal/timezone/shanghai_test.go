package timezone

import (
	"testing"
	"time"
)

func TestShanghaiDateAtCrossesDayBeforeUTC(t *testing.T) {
	utc := time.Date(2026, 7, 30, 16, 23, 0, 0, time.UTC)
	if got := DateAt(utc); got != "2026-07-31" {
		t.Fatalf("DateAt() = %q, want 2026-07-31", got)
	}
}

func TestDayBoundsAtUseShanghaiMidnight(t *testing.T) {
	start, end := DayBoundsAt(time.Date(2026, 7, 30, 16, 23, 0, 0, time.UTC))
	if got := start.Format(time.RFC3339); got != "2026-07-31T00:00:00+08:00" {
		t.Fatalf("start = %q", got)
	}
	if got := end.Format(time.RFC3339Nano); got != "2026-07-31T23:59:59.999999999+08:00" {
		t.Fatalf("end = %q", got)
	}
}
