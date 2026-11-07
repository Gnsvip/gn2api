package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const grokResponsesClientToolMappingContextKey = "grok_responses_client_tool_mapping"

func adaptResponsesClientToolsForFunctionUpstream(body []byte, upstream string) ([]byte, apicompat.ResponsesClientToolMapping, error) {
	return adaptResponsesClientToolsForFunctionUpstreamWithMapping(
		body,
		upstream,
		apicompat.ResponsesClientToolMapping{},
	)
}

func adaptResponsesClientToolsForFunctionUpstreamWithMapping(
	body []byte,
	upstream string,
	inherited apicompat.ResponsesClientToolMapping,
	inheritedLoweredTools ...[]any,
) ([]byte, apicompat.ResponsesClientToolMapping, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var requestBody map[string]any
	if err := decoder.Decode(&requestBody); err != nil {
		return body, apicompat.ResponsesClientToolMapping{}, fmt.Errorf("decode %s Responses client tools: %w", upstream, err)
	}

	mapping, changed, err := apicompat.AdaptResponsesClientToolsWithInheritedMapping(requestBody, inherited, inheritedLoweredTools...)
	if err != nil {
		return body, apicompat.ResponsesClientToolMapping{}, err
	}
	if !changed {
		return body, mapping, nil
	}
	rebuilt, err := marshalOpenAIUpstreamJSON(requestBody)
	if err != nil {
		return body, apicompat.ResponsesClientToolMapping{}, fmt.Errorf("encode %s Responses client tools: %w", upstream, err)
	}
	return rebuilt, mapping, nil
}

func adaptGrokResponsesClientTools(body []byte) ([]byte, apicompat.ResponsesClientToolMapping, error) {
	return adaptResponsesClientToolsForFunctionUpstream(body, "Grok")
}

func hasResponsesClientToolMapping(mapping apicompat.ResponsesClientToolMapping) bool {
	return len(mapping.CustomTools) > 0 || mapping.ToolSearch || len(mapping.NamespaceTools) > 0
}

func hasGrokResponsesClientToolMapping(mapping apicompat.ResponsesClientToolMapping) bool {
	return hasResponsesClientToolMapping(mapping)
}

func setGrokResponsesClientToolMapping(c *gin.Context, mapping apicompat.ResponsesClientToolMapping) {
	if c == nil {
		return
	}
	if !hasGrokResponsesClientToolMapping(mapping) {
		clearGrokResponsesClientToolMapping(c)
		return
	}
	c.Set(grokResponsesClientToolMappingContextKey, mapping)
}

func clearGrokResponsesClientToolMapping(c *gin.Context) {
	if c == nil {
		return
	}
	if _, exists := c.Get(grokResponsesClientToolMappingContextKey); !exists {
		return
	}
	c.Set(grokResponsesClientToolMappingContextKey, apicompat.ResponsesClientToolMapping{})
}

func grokResponsesClientToolMapping(c *gin.Context) (apicompat.ResponsesClientToolMapping, bool) {
	if c == nil {
		return apicompat.ResponsesClientToolMapping{}, false
	}
	value, ok := c.Get(grokResponsesClientToolMappingContextKey)
	if !ok {
		return apicompat.ResponsesClientToolMapping{}, false
	}
	mapping, ok := value.(apicompat.ResponsesClientToolMapping)
	return mapping, ok && hasGrokResponsesClientToolMapping(mapping)
}

func restoreGrokResponsesClientToolPayload(c *gin.Context, payload []byte) ([]byte, error) {
	mapping, ok := grokResponsesClientToolMapping(c)
	if !ok || !bytes.Contains(payload, []byte(`"function_call"`)) || !json.Valid(payload) {
		return payload, nil
	}
	restored, _, err := apicompat.RestoreResponsesClientToolPayload(payload, mapping)
	return restored, err
}

type responsesClientToolStreamBody struct {
	*io.PipeReader
	source io.Closer
}

// Drain already received tool deltas before the scanner may block on upstream
// input. This batches bursts without a timer or delaying paced output.
type responsesToolFlushReader struct {
	io.Reader
	flush func() error
}

func (r *responsesToolFlushReader) Read(p []byte) (int, error) {
	if r.flush != nil {
		if err := r.flush(); err != nil {
			return 0, err
		}
	}
	return r.Reader.Read(p)
}

func (b *responsesClientToolStreamBody) Close() error {
	readerErr := b.PipeReader.Close()
	sourceErr := b.source.Close()
	if readerErr != nil {
		return readerErr
	}
	return sourceErr
}

func newResponsesClientToolStreamBody(
	source io.ReadCloser,
	mapping apicompat.ResponsesClientToolMapping,
	maxLineSize int,
	cfg *config.Config,
	timing ...*requesttiming.Attempt,
) io.ReadCloser {
	reader, writer := io.Pipe()
	body := &responsesClientToolStreamBody{PipeReader: reader, source: source}
	go transformResponsesClientToolStream(source, writer, mapping, maxLineSize, cfg, timing...)
	return body
}

func newGrokResponsesClientToolStreamBody(
	source io.ReadCloser,
	mapping apicompat.ResponsesClientToolMapping,
	maxLineSize int,
	cfg *config.Config,
	timing ...*requesttiming.Attempt,
) io.ReadCloser {
	return newResponsesClientToolStreamBody(source, mapping, maxLineSize, cfg, timing...)
}

func transformResponsesClientToolStream(
	source io.ReadCloser,
	destination *io.PipeWriter,
	mapping apicompat.ResponsesClientToolMapping,
	maxLineSize int,
	cfg *config.Config,
	timing ...*requesttiming.Attempt,
) {
	defer func() { _ = source.Close() }()
	if maxLineSize <= 0 {
		maxLineSize = defaultMaxLineSize
	}

	input := &responsesToolFlushReader{Reader: source}
	scanner := bufio.NewScanner(input)
	scanBuf := getSSEScannerBuf64K()
	defer putSSEScannerBuf64K(scanBuf)
	scanner.Buffer(scanBuf[:0], maxLineSize)
	scanner.Split(newBoundedSSESplit(cfg, maxLineSize, nil))
	documents := newOpenAISSEJSONDocumentScanner(scanner)
	restorer := apicompat.NewResponsesClientToolStreamRestorer(mapping)
	buffered := bufio.NewWriterSize(destination, 4*1024)
	pendingFields := make([]string, 0, 2)
	frameHadEventField := false
	frameEmitted := false
	var attempt *requesttiming.Attempt
	if len(timing) > 0 {
		attempt = timing[0]
	}
	rawContentObserved, outputContentObserved := false, false
	observe := func(payload []byte, raw bool) {
		if attempt == nil || (raw && rawContentObserved) || (!raw && outputContentObserved) || !json.Valid(payload) {
			return
		}
		eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
		if eventType == "" {
			for _, field := range pendingFields {
				if value, ok := extractOpenAISSEEventLine(field); ok {
					eventType = value
				}
			}
		}
		content := openAIStreamDataStartsTTFT(string(payload), eventType, false, OpenAITTFTModeVisible)
		attempt.ToolAdapterEvent(raw, eventType, content)
		if raw {
			rawContentObserved = content
		} else {
			outputContentObserved = content
		}
	}

	writeLine := func(line string) error {
		if _, err := buffered.WriteString(line); err != nil {
			return err
		}
		return buffered.WriteByte('\n')
	}
	writePendingFields := func(payload []byte, fields []string, includeNonEvent bool) error {
		eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
		for _, field := range fields {
			if _, isEvent := extractOpenAISSEEventLine(field); isEvent {
				if eventType != "" {
					if err := writeLine("event: " + eventType); err != nil {
						return err
					}
				} else if err := writeLine(field); err != nil {
					return err
				}
				continue
			}
			if includeNonEvent {
				if err := writeLine(field); err != nil {
					return err
				}
			}
		}
		return nil
	}
	writePayloads := func(payloads [][]byte, fields []string, hadEvent bool) error {
		for index, payload := range payloads {
			observe(payload, false)
			if index == 0 {
				if err := writePendingFields(payload, fields, true); err != nil {
					return err
				}
			} else if hadEvent {
				eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
				if eventType != "" {
					if err := writeLine("event: " + eventType); err != nil {
						return err
					}
				}
			}
			if err := writeLine("data: " + string(payload)); err != nil {
				return err
			}
			if err := writeLine(""); err != nil {
				return err
			}
		}
		return buffered.Flush()
	}
	const batchLimit = 16 * 1024
	var batch apicompat.ResponsesStreamEvent
	var batchDelta strings.Builder
	var batchFields []string
	var batchHadEvent bool
	flushBatch := func() error {
		if batchDelta.Len() == 0 {
			return nil
		}
		batch.Delta = batchDelta.String()
		payload, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		payloads, _, err := restorer.RestoreEvent(payload)
		if err != nil {
			return err
		}
		batchDelta.Reset()
		return writePayloads(payloads, batchFields, batchHadEvent)
	}
	input.flush = flushBatch

	for documents.Scan() {
		line := documents.Text()
		data, isData := extractOpenAISSEDataLine(line)
		if isData {
			payload := []byte(data)
			observe(payload, true)
			if gjson.GetBytes(payload, "type").String() == "response.function_call_arguments.delta" {
				var event apicompat.ResponsesStreamEvent
				if json.Unmarshal(payload, &event) == nil && event.Delta != "" && restorer.CanBatchCustomInputDelta(event) {
					if batchDelta.Len() > 0 && (batch.ItemID != event.ItemID || batch.CallID != event.CallID ||
						batch.OutputIndex != event.OutputIndex || !slices.Equal(batchFields, pendingFields) ||
						batchDelta.Len()+len(event.Delta) > batchLimit) {
						if err := flushBatch(); err != nil {
							_ = destination.CloseWithError(err)
							return
						}
					}
					if batchDelta.Len() == 0 {
						batch, batchHadEvent = event, frameHadEventField
						batchFields = append(batchFields[:0], pendingFields...)
					}
					batchDelta.WriteString(event.Delta)
					if batchDelta.Len() >= batchLimit {
						if err := flushBatch(); err != nil {
							_ = destination.CloseWithError(err)
							return
						}
					}
					pendingFields = pendingFields[:0]
					frameHadEventField, frameEmitted = false, true
					continue
				}
			}
			if err := flushBatch(); err != nil {
				_ = destination.CloseWithError(err)
				return
			}
			payloads := [][]byte{payload}
			if json.Valid(payload) {
				var err error
				payloads, _, err = restorer.RestoreEvent(payload)
				if err != nil {
					_ = buffered.Flush()
					_ = destination.CloseWithError(fmt.Errorf("restore Responses client tool event: %w", err))
					return
				}
			}
			if err := writePayloads(payloads, pendingFields, frameHadEventField); err != nil {
				_ = destination.CloseWithError(err)
				return
			}
			pendingFields = pendingFields[:0]
			frameHadEventField = false
			frameEmitted = true
			continue
		}

		if line == "" {
			if !frameEmitted {
				if err := flushBatch(); err != nil {
					_ = destination.CloseWithError(err)
					return
				}
				for _, field := range pendingFields {
					if err := writeLine(field); err != nil {
						_ = destination.CloseWithError(err)
						return
					}
				}
				if len(pendingFields) > 0 {
					if err := writeLine(""); err != nil {
						_ = destination.CloseWithError(err)
						return
					}
					if err := buffered.Flush(); err != nil {
						_ = destination.CloseWithError(err)
						return
					}
				}
			}
			pendingFields = pendingFields[:0]
			frameHadEventField = false
			frameEmitted = false
			continue
		}

		if _, isEvent := extractOpenAISSEEventLine(line); isEvent {
			frameHadEventField = true
		}
		pendingFields = append(pendingFields, line)
	}

	if err := flushBatch(); err != nil {
		_ = destination.CloseWithError(err)
		return
	}
	for _, field := range pendingFields {
		if err := writeLine(field); err != nil {
			_ = destination.CloseWithError(err)
			return
		}
	}
	if err := buffered.Flush(); err != nil {
		_ = destination.CloseWithError(err)
		return
	}
	if err := documents.Err(); err != nil {
		_ = destination.CloseWithError(err)
		return
	}
	_ = destination.Close()
}
