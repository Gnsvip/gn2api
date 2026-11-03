package service

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gin-gonic/gin"
)

// Track byte offsets, not body copies. bufio may write before an explicit Flush;
// only the write that actually includes the first content data line is marked.
type openAIContentTimingWriter struct {
	gin.ResponseWriter
	timing               *requesttiming.Attempt
	written, contentEnd  int64
	identified, observed bool
}

func (w *openAIContentTimingWriter) expectContent(pending int64, dataBytes int) {
	if !w.identified {
		w.identified = true
		w.contentEnd = w.written + pending + int64(dataBytes)
	}
}
func (w *openAIContentTimingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.written += int64(n)
	if w.identified && !w.observed && w.written >= w.contentEnd {
		w.observed = true
		w.timing.Written(true)
	}
	if err != nil {
		w.timing.Fail("downstream_write_error")
	}
	return n, err
}
func (w *openAIContentTimingWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }
