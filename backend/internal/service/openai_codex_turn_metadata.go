package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf16"
)

// Turn metadata is also used as an HTTP header, including when carried in WS JSON.
func marshalCodexTurnMetadata(metadata map[string]any) ([]byte, error) {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	for i, b := range raw {
		if b < 0x7f {
			continue
		}
		out := make([]byte, 0, len(raw)+16)
		out = append(out, raw[:i]...)
		appendEscape := func(r rune) {
			const hex = "0123456789abcdef"
			out = append(out, '\\', 'u', hex[r>>12&15], hex[r>>8&15], hex[r>>4&15], hex[r&15])
		}
		for _, r := range string(raw[i:]) {
			switch {
			case r < 0x7f:
				out = append(out, byte(r))
			case r <= 0xffff:
				appendEscape(r)
			default:
				high, low := utf16.EncodeRune(r)
				appendEscape(high)
				appendEscape(low)
			}
		}
		return out, nil
	}
	return raw, nil
}

// writeCodexJSONString 只转义 JSON 语法必需的字节；非 ASCII 留给
// escapeCodexNonASCII 统一处理（官方 AsciiJsonFormatter 也是两层语义）。
// 与官方一致：& < > 不转义。
//
//nolint:errcheck // bytes.Buffer 写入不会失败
func writeCodexJSONString(buf *bytes.Buffer, value string) {
	buf.WriteByte('"')
	start := 0
	for i := range len(value) {
		switch b := value[i]; b {
		case '"', '\\':
			buf.WriteString(value[start:i])
			buf.WriteByte('\\')
			buf.WriteByte(b)
			start = i + 1
		case '\n':
			buf.WriteString(value[start:i])
			buf.WriteString(`\n`)
			start = i + 1
		case '\r':
			buf.WriteString(value[start:i])
			buf.WriteString(`\r`)
			start = i + 1
		case '\t':
			buf.WriteString(value[start:i])
			buf.WriteString(`\t`)
			start = i + 1
		default:
			if b < 0x20 {
				buf.WriteString(value[start:i])
				fmt.Fprintf(buf, `\u%04x`, b)
				start = i + 1
			}
		}
	}
	buf.WriteString(value[start:])
	buf.WriteByte('"')
}

// escapeCodexNonASCII 复刻官方 to_ascii_json_string 的非 ASCII 转义
// （codex-rs/utils/string/src/json.rs:12-38）：只把非 ASCII 码点写成小写 \uXXXX，
// ASCII（含 & < > 与 U+007F）原样保留。官方仅在字符串片段上做该转义，
// 而合法 JSON 里的高位字节只可能出现在字符串内，故字节级后处理等价。
func escapeCodexNonASCII(raw []byte) []byte {
	for i, b := range raw {
		if b < 0x7f {
			continue
		}
		out := make([]byte, 0, len(raw)+16)
		out = append(out, raw[:i]...)
		appendEscape := func(r rune) {
			const hex = "0123456789abcdef"
			out = append(out, '\\', 'u', hex[r>>12&15], hex[r>>8&15], hex[r>>4&15], hex[r&15])
		}
		for _, r := range string(raw[i:]) {
			switch {
			case r < 0x7f:
				out = append(out, byte(r))
			case r <= 0xffff:
				appendEscape(r)
			default:
				high, low := utf16.EncodeRune(r)
				appendEscape(high)
				appendEscape(low)
			}
		}
		return out
	}
	return raw
}
