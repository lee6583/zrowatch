package timezone

import (
	"time"
	_ "time/tzdata"
)

const (
	Name       = "Asia/Shanghai"
	DateLayout = "2006-01-02"
)

var shanghai = mustLoadShanghai()

func mustLoadShanghai() *time.Location {
	location, err := time.LoadLocation(Name)
	if err != nil {
		panic("load Asia/Shanghai timezone: " + err.Error())
	}
	return location
}

func Location() *time.Location {
	return shanghai
}

func Now() time.Time {
	return time.Now().In(shanghai)
}

func Today() string {
	return Now().Format(DateLayout)
}

func DateAt(value time.Time) string {
	return value.In(shanghai).Format(DateLayout)
}

func ParseDate(value string) (time.Time, error) {
	return time.ParseInLocation(DateLayout, value, shanghai)
}

func DayBoundsAt(value time.Time) (time.Time, time.Time) {
	local := value.In(shanghai)
	start := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, shanghai)
	return start, start.AddDate(0, 0, 1).Add(-time.Nanosecond)
}
