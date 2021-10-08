package source

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jinzhu/now"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

// ParseFilters parses a list of filters in strings into LogEntryFilters.
func ParseFilters(strs ...string) (LogEntryFilters, error) {
	filters := make([]LogEntryFilter, 0, len(strs))
	for n, str := range strs {
		f, err := ParseFilter(str)
		if err != nil {
			return nil, fmt.Errorf("invalid filter %d: %w", n, err)
		}
		if f != nil {
			filters = append(filters, f)
		}
	}
	return LogEntryFilters(filters), nil
}

// ParseFilter parses a string into a LogEntryFilter.
func ParseFilter(str string) (LogEntryFilter, error) {
	tokens := strings.SplitN(str, "=", 2)

	if len(tokens) == 1 {
		if tokens[0] == "" {
			return nil, nil
		}
		return MessageContains(tokens[0]), nil
	}

	val := tokens[1]
	switch strings.ToLower(tokens[0]) {
	case "since":
		t, err := parseTime(val)
		if err != nil {
			return nil, err
		}
		return FilterSince(t), nil
	case "before":
		t, err := parseTime(val)
		if err != nil {
			return nil, err
		}
		return FilterBefore(t), nil
	case "l", "lv", "level":
		level, err := logs.ParseLevel(val)
		if err != nil {
			return nil, err
		}
		if level == logspb.LogEntry_NONE {
			return nil, nil
		}
		return FilterByLevel(level), nil
	case "location", "loc":
		if val == "" {
			return nil, nil
		}
		if strings.HasPrefix(val, "!") || strings.HasPrefix(val, "~") {
			return LocationNotContains(val[1:]), nil
		}
		return LocationContains(val), nil
	case "span-events", "span-event", "event", "se", "ev":
		switch strings.ToLower(val) {
		case "", "no", "none":
			return ExcludeSpanEvents(), nil
		}
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown filter: %s", str)
	}
}

func parseTime(str string) (time.Time, error) {
	nanos, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		return time.Unix(0, nanos), nil
	}
	t, err := now.Parse(str)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}
