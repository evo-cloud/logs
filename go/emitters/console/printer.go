package console

import (
	"encoding/hex"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

// Decorations.
const (
	decorKey       = "\x1b[34m" // fg:blue
	decorTrue      = "\x1b[32m" // fg:green
	decorFalse     = "\x1b[31m" // fg:red
	decorInt       = "\x1b[96m" // fg:cyan-light
	decorFloat     = "\x1b[36m" // fg:cyan
	decorDouble    = "\x1b[36m" // fg:cyan
	decorStr       = "\x1b[94m" // fg:blue-light
	decorJSON      = "\x1b[33m" // fg:yellow
	decorProto     = "\x1b[37m" // fg:white
	decorTraceID   = "\x1b[35m" // fg:magenta
	decorSpanID    = "\x1b[36m" // fg:cyan
	decorSpanName  = "\x1b[32m" // fg:green
	decorSpanStart = "\x1b[92m" // fg:green-light
	decorSpanEnd   = "\x1b[92m" // fg:green-light
	decorLoc       = "\x1b[2m"  // dim
)

var (
	levelFmts = map[logspb.LogEntry_Level]*levelFmt{
		logspb.LogEntry_INFO:     {decor: "\x1b[37m", text: "I"},
		logspb.LogEntry_WARNING:  {decor: "\x1b[33m", text: "W"},
		logspb.LogEntry_ERROR:    {decor: "\x1b[31m", text: "E"},
		logspb.LogEntry_CRITICAL: {decor: "\x1b[31m\x1b[1m", text: "C"},
		logspb.LogEntry_FATAL:    {decor: "\x1b[31m\x1b[1m\x1b[5m", text: "F"},
	}
)

type levelFmt struct {
	decor string
	text  string
}

// Printer prints log entries to console in a human readable format.
type Printer struct {
	Out io.Writer

	MaxStrAttrLen  int
	MaxBinAttrLen  int
	MaxPathLen     int
	ShortenTraceID bool
	DisplayNanoTS  bool
	TimeFormat     string

	styler      func(text, decor string) string
	useSpansMap bool
	spansLock   sync.RWMutex
	spans       map[string]*logspb.Trace_SpanStart
}

// SpanRecorder is used to remove the tracked span event when it ends.
type SpanRecorder struct {
	printer   *Printer
	endSpanID string
}

// NewPrinter creates a Printer with default configurations.
func NewPrinter(out io.Writer) *Printer {
	return &Printer{
		Out:            out,
		MaxStrAttrLen:  80,
		MaxBinAttrLen:  8,
		MaxPathLen:     20,
		ShortenTraceID: true,
		TimeFormat:     "0102 15:04:05.000000",
		styler:         noColorStyler,
	}
}

// UseColor enables/disables colorful output.
func (p *Printer) UseColor(colorful bool) {
	if colorful {
		p.styler = colorfulStyler
	} else {
		p.styler = noColorStyler
	}
}

// DisplaySpanNames enables span event tracking for displaying span names in the
// related logs.
func (p *Printer) DisplaySpanNames() {
	p.useSpansMap = true
}

// RecordSpanEvent tracks span events for displaying span names in future.
// It does nothing until DisplaySpanNames is called to enable this feature.
// The application should call Done of the returned SpanRecorder (even if it's nil).
func (p *Printer) RecordSpanEvent(entry *logspb.LogEntry) *SpanRecorder {
	if !p.useSpansMap {
		return nil
	}
	spanCtx := entry.GetTrace().GetSpanContext()
	if spanCtx == nil {
		return nil
	}
	switch ev := entry.GetTrace().GetEvent().(type) {
	case *logspb.Trace_SpanStart_:
		id := logs.IDStringFrom(spanCtx)
		p.spansLock.Lock()
		if p.spans == nil {
			p.spans = make(map[string]*logspb.Trace_SpanStart)
		}
		p.spans[id] = ev.SpanStart
		p.spansLock.Unlock()
	case *logspb.Trace_SpanEnd_:
		return &SpanRecorder{printer: p, endSpanID: logs.IDStringFrom(spanCtx)}
	}
	return nil
}

// Done completes the SpanRecorder.
func (r *SpanRecorder) Done() {
	if r == nil || r.endSpanID == "" {
		return
	}
	r.printer.spansLock.Lock()
	defer r.printer.spansLock.Unlock()
	if r.printer.spans != nil {
		delete(r.printer.spans, r.endSpanID)
	}
}

// EmitLogEntry implements LogEmitter.
func (p *Printer) EmitLogEntry(entry *logspb.LogEntry) {
	var sb strings.Builder
	var levelDecor string
	if f := levelFmts[entry.GetLevel()]; f != nil {
		levelDecor = f.decor
		sb.WriteString(p.styler(f.text, f.decor))
	} else {
		sb.WriteString(" ")
	}
	if p.DisplayNanoTS {
		sb.WriteString(strconv.FormatInt(entry.GetNanoTs(), 10))
	} else {
		sb.WriteString(time.Unix(0, entry.GetNanoTs()).Format(p.TimeFormat))
	}
	sb.WriteByte(' ')
	if loc := entry.GetLocation(); loc != "" {
		if p.MaxPathLen == 0 {
			loc = filepath.Base(loc)
		} else if p.MaxPathLen > 0 && len(loc) > p.MaxPathLen {
			loc = ".." + loc[len(loc)-p.MaxPathLen:]
		}
		sb.WriteString(p.styler(loc, decorLoc))
		sb.WriteByte(' ')
	}
	tr := entry.GetTrace()
	if event := tr.GetEvent(); event != nil {
		switch ev := event.(type) {
		case *logspb.Trace_SpanStart_:
			sb.WriteString(p.styler("+ "+ev.SpanStart.GetName(), decorSpanStart))
		case *logspb.Trace_SpanEnd_:
			text := "-"
			if span := p.lookupSpan(tr.GetSpanContext()); span != nil {
				text += " " + span.GetName()
			}
			sb.WriteString(p.styler(text, decorSpanEnd))
		}
	} else {
		sb.WriteString(p.styler(entry.GetMessage(), levelDecor))
	}
	for key, val := range entry.GetAttributes() {
		sb.WriteByte(' ')
		sb.WriteString(p.styler(key, decorKey))
		sb.WriteByte('=')
		switch v := val.GetValue().(type) {
		case *logspb.Value_BoolValue:
			if v.BoolValue {
				sb.WriteString(p.styler("T", decorTrue))
			} else {
				sb.WriteString(p.styler("F", decorFalse))
			}
		case *logspb.Value_IntValue:
			sb.WriteString(p.styler(strconv.FormatInt(v.IntValue, 10), decorInt))
		case *logspb.Value_FloatValue:
			sb.WriteString(p.styler(strconv.FormatFloat(float64(v.FloatValue), 'E', 8, 32), decorFloat))
		case *logspb.Value_DoubleValue:
			sb.WriteString(p.styler(strconv.FormatFloat(float64(v.DoubleValue), 'E', 8, 64), decorDouble))
		case *logspb.Value_StrValue:
			sb.WriteString(p.styler(p.trimStrAttrValue(v.StrValue), decorStr))
		case *logspb.Value_Json:
			sb.WriteString(p.styler(p.trimStrAttrValue(v.Json), decorJSON))
		case *logspb.Value_Proto:
			maxBinLen := 8
			if p.MaxBinAttrLen > 0 {
				maxBinLen = p.MaxBinAttrLen
			}
			var str string
			if len(v.Proto) > maxBinLen {
				str = hex.EncodeToString(v.Proto[:8]) + "..."
			} else {
				str = hex.EncodeToString(v.Proto)
			}
			sb.WriteString(p.styler(str, decorProto))
		}
	}
	if spanCtx := tr.GetSpanContext(); spanCtx != nil {
		traceID, spanID := logs.TraceIDStringFrom(spanCtx), logs.SpanIDStringFrom(spanCtx)
		if p.ShortenTraceID && len(traceID) >= 10 {
			traceID = traceID[:6] + ".." + traceID[len(traceID)-4:]
		}
		if p.ShortenTraceID && len(spanID) >= 6 {
			// SpanID is using unix nano timestamp. The MSBs are mostly identical.
			spanID = ".." + spanID[len(spanID)-6:]
		}
		sb.WriteByte(' ')
		sb.WriteString(p.styler(traceID, decorTraceID))
		sb.WriteByte('/')
		sb.WriteString(p.styler(spanID, decorSpanID))
		if span := p.lookupSpan(spanCtx); span != nil {
			sb.WriteByte(' ')
			sb.WriteString(p.styler(span.GetName(), decorSpanName))
		}
	}

	sb.WriteString("\r\n")
	io.WriteString(p.Out, sb.String())
}

func (p *Printer) trimStrAttrValue(val string) string {
	if p.MaxStrAttrLen > 0 && p.MaxStrAttrLen < len(val) {
		return val[:p.MaxStrAttrLen] + "..."
	}
	return val
}

func (p *Printer) lookupSpan(spanCtx *logspb.SpanContext) *logspb.Trace_SpanStart {
	if spanCtx == nil || !p.useSpansMap {
		return nil
	}
	p.spansLock.RLock()
	span := p.spans[logs.IDStringFrom(spanCtx)]
	p.spansLock.RUnlock()
	return span
}

func colorfulStyler(text, decor string) string {
	if decor == "" {
		return text
	}
	return decor + text + "\x1b[0m"
}

func noColorStyler(text, decor string) string {
	return text
}
