package source

import (
	"testing"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

func logEntryWith(setters ...logs.AttributeSetter) *logspb.LogEntry {
	entry := &logspb.LogEntry{Attributes: make(map[string]*logspb.Value)}
	logs.AttributeSetters(setters).SetAttributes(entry.Attributes)
	return entry
}

func TestFilters(t *testing.T) {
	testCases := []struct {
		filter string
		entry  *logspb.LogEntry
		match  bool
	}{
		{
			filter: "a:key=value",
			entry:  logEntryWith(logs.Str("key", "value")),
			match:  true,
		},
		{
			filter: "a:key!=value",
			entry:  logEntryWith(logs.Str("key", "value")),
		},
		{
			filter: "a:key<=a",
			entry:  logEntryWith(logs.Str("key", "a")),
			match:  true,
		},
		{
			filter: "a:key>=a",
			entry:  logEntryWith(logs.Str("key", "a")),
			match:  true,
		},
		{
			filter: "a:key<a",
			entry:  logEntryWith(logs.Str("key", "a")),
		},
		{
			filter: "a:key>a",
			entry:  logEntryWith(logs.Str("key", "a")),
		},
		{
			filter: "a:key:value",
			entry:  logEntryWith(logs.Str("key", "value")),
			match:  true,
		},
		{
			filter: "a:key:al",
			entry:  logEntryWith(logs.Str("key", "value")),
			match:  true,
		},
		{
			filter: "a:key:c",
			entry:  logEntryWith(logs.Str("key", "value")),
		},
		{
			filter: "a:key~val",
			entry:  logEntryWith(logs.Str("key", "value")),
			match:  true,
		},
		{
			filter: "a:key~val$",
			entry:  logEntryWith(logs.Str("key", "value")),
		},
		{
			filter: "a:key~^v.+e$",
			entry:  logEntryWith(logs.Str("key", "value")),
			match:  true,
		},
		{
			filter: "a:nonexist=",
			entry:  logEntryWith(logs.Str("key", "value")),
			match:  true,
		},
		{
			filter: "a:nonexist=value",
			entry:  logEntryWith(logs.Str("key", "value")),
		},
		// bool values.
		{
			filter: "a:key=true",
			entry:  logEntryWith(logs.Bool("key", true)),
			match:  true,
		},
		{
			filter: "a:key=false",
			entry:  logEntryWith(logs.Bool("key", false)),
			match:  true,
		},
		{
			filter: "a:nonexist=false",
			entry:  logEntryWith(logs.Bool("key", true)),
		},
		{
			filter: "a:key!=false",
			entry:  logEntryWith(logs.Bool("key", true)),
			match:  true,
		},
		{
			filter: "a:key<=true",
			entry:  logEntryWith(logs.Bool("key", true)),
		},
		{
			filter: "a:key>=true",
			entry:  logEntryWith(logs.Bool("key", true)),
		},
		{
			filter: "a:key<true",
			entry:  logEntryWith(logs.Bool("key", true)),
		},
		{
			filter: "a:key>true",
			entry:  logEntryWith(logs.Bool("key", true)),
		},
		// float values.
		{
			filter: "a:key=1.0",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key!=0.0",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key=1",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key=1e0",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key<=1e0",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key>=1e0",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key<1e0",
			entry:  logEntryWith(logs.Float("key", 1.0)),
		},
		{
			filter: "a:key>1e0",
			entry:  logEntryWith(logs.Float("key", 1.0)),
		},
		{
			filter: "a:key!=1e0",
			entry:  logEntryWith(logs.Float("key", 1.0)),
		},
		{
			filter: "a:key=u1",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key<0x2",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key>-1",
			entry:  logEntryWith(logs.Float("key", 1.0)),
			match:  true,
		},
		{
			filter: "a:key>u1",
			entry:  logEntryWith(logs.Float("key", 1.0)),
		},
		{
			filter: "a:key=1.0",
			entry:  logEntryWith(logs.Double("key", 1.0)),
			match:  true,
		},
		// int values.
		{
			filter: "a:key=10",
			entry:  logEntryWith(logs.Int("key", 10)),
			match:  true,
		},
		{
			filter: "a:key!=1",
			entry:  logEntryWith(logs.Int("key", 10)),
			match:  true,
		},
		{
			filter: "a:key!=10",
			entry:  logEntryWith(logs.Int("key", 10)),
		},
		{
			filter: "a:key=1e1",
			entry:  logEntryWith(logs.Int("key", 10)),
			match:  true,
		},
		{
			filter: "a:key<=1e1",
			entry:  logEntryWith(logs.Int("key", 10)),
			match:  true,
		},
		{
			filter: "a:key>=1e1",
			entry:  logEntryWith(logs.Int("key", 10)),
			match:  true,
		},
		{
			filter: "a:key<1e1",
			entry:  logEntryWith(logs.Int("key", 10)),
		},
		{
			filter: "a:key>1e1",
			entry:  logEntryWith(logs.Int("key", 10)),
		},
		{
			filter: "a:key>10",
			entry:  logEntryWith(logs.Int("key", 10)),
		},
		{
			filter: "a:key<10",
			entry:  logEntryWith(logs.Int("key", 10)),
		},
		{
			filter: "a:key>u1",
			entry:  logEntryWith(logs.Int("key", -1)),
			match:  true,
		},
		{
			filter: "a:key>0xa",
			entry:  logEntryWith(logs.Int("key", -1)),
			match:  true,
		},
		{
			filter: "a:key>1",
			entry:  logEntryWith(logs.Int("key", -1)),
		},
	}
	for n := range testCases {
		tc := testCases[n]
		t.Run(tc.filter, func(t *testing.T) {
			f, err := ParseFilter(tc.filter)
			if err != nil {
				t.Errorf("parse filter %q: %v", tc.filter, err)
				return
			}
			if match := f.FilterLogEntry(tc.entry); match != tc.match {
				t.Errorf("Expect match=%v, got %v", tc.match, match)
			}
		})
	}
}
