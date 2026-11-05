package apicompat

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Decode only the input string of the function wrapper we generate. Other
// wrapper shapes retain the existing completion-time compatibility fallback.
// The cursor advances once per byte; split escapes/runes wait for the next delta.
type customInputStream struct {
	pos      int
	stage    int
	fallback bool
	emitted  strings.Builder
}

func (s *customInputStream) append(arguments string) (string, error) {
	if s.fallback || s.stage == 5 {
		return "", nil
	}
	for s.stage < 4 {
		for s.pos < len(arguments) && strings.ContainsRune(" \r\n\t", rune(arguments[s.pos])) {
			s.pos++
		}
		want := []string{"{", `"input"`, ":", `"`}[s.stage]
		remaining := arguments[s.pos:]
		if len(remaining) < len(want) && strings.HasPrefix(want, remaining) {
			return "", nil
		}
		if !strings.HasPrefix(remaining, want) {
			s.fallback = true
			return "", nil
		}
		s.pos += len(want)
		s.stage++
	}
	var delta strings.Builder
	for s.pos < len(arguments) {
		start := s.pos
		b := arguments[start]
		if b == '"' {
			s.pos++
			s.stage = 5
			break
		}
		if b < 0x20 {
			return "", errors.New("invalid control character in custom tool input")
		}
		if b == '\\' {
			if len(arguments)-start < 2 {
				break
			}
			if arguments[start+1] == 'u' {
				if len(arguments)-start < 6 {
					break
				}
				value, err := strconv.ParseUint(arguments[start+2:start+6], 16, 16)
				if err != nil {
					return "", errors.New("invalid unicode escape in custom tool input")
				}
				r := rune(value)
				n := 6
				if utf16.IsSurrogate(r) {
					if r >= 0xDC00 {
						return "", errors.New("unpaired unicode surrogate in custom tool input")
					}
					if len(arguments)-start < 12 {
						break
					}
					low, err := strconv.ParseUint(arguments[start+8:start+12], 16, 16)
					if arguments[start+6:start+8] != `\u` || err != nil || low < 0xDC00 || low > 0xDFFF {
						return "", errors.New("unpaired unicode surrogate in custom tool input")
					}
					r = utf16.DecodeRune(r, rune(low))
					n = 12
				}
				delta.WriteRune(r)
				s.pos += n
				continue
			}
			var decoded byte
			switch arguments[start+1] {
			case '"', '\\', '/':
				decoded = arguments[start+1]
			case 'b':
				decoded = '\b'
			case 'f':
				decoded = '\f'
			case 'n':
				decoded = '\n'
			case 'r':
				decoded = '\r'
			case 't':
				decoded = '\t'
			default:
				return "", errors.New("invalid escape in custom tool input")
			}
			delta.WriteByte(decoded)
			s.pos += 2
			continue
		}
		if !utf8.FullRuneInString(arguments[start:]) {
			break
		}
		r, n := utf8.DecodeRuneInString(arguments[start:])
		if r == utf8.RuneError && n == 1 {
			return "", errors.New("invalid UTF-8 in custom tool input")
		}
		delta.WriteString(arguments[start : start+n])
		s.pos += n
	}
	text := delta.String()
	s.emitted.WriteString(text)
	return text, nil
}

// A done event may provide an authoritative full value instead of the remaining
// deltas. Never repeat already delivered bytes or accept a rewritten prefix.
func (s *customInputStream) finish(arguments string) (string, string, error) {
	input := extractCustomToolCallInput(arguments)
	if s.stage >= 4 {
		var err error
		input, err = uniqueCustomInput(arguments)
		if err != nil {
			return "", "", err
		}
	}
	if !strings.HasPrefix(input, s.emitted.String()) {
		return "", "", errors.New("custom tool completion disagrees with streamed input")
	}
	delta := input[s.emitted.Len():]
	s.emitted.WriteString(delta)
	return delta, input, nil
}

// encoding/json alone accepts duplicate keys. Reject duplicate input fields
// before completing a streamed tool so a later value cannot replace its prefix.
func uniqueCustomInput(arguments string) (string, error) {
	invalid := errors.New("invalid or ambiguous custom tool input wrapper")
	d := json.NewDecoder(strings.NewReader(arguments))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return "", invalid
	}
	seen := false
	input := ""
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return "", invalid
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return "", invalid
		}
		if key == "input" {
			if seen || len(value) == 0 || value[0] != '"' || json.Unmarshal(value, &input) != nil {
				return "", invalid
			}
			seen = true
		}
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') || !seen {
		return "", invalid
	}
	if _, err := d.Token(); err != io.EOF {
		return "", invalid
	}
	return input, nil
}
