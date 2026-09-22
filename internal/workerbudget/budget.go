// Package workerbudget implements the conservative size budgets enforced
// before a dynamic WorkerCode reaches stock workerd's workerLoader.
package workerbudget

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	CodeMaxBytes        int64 = 64 * 1024 * 1024
	EnvUpstreamMaxBytes int64 = 1024 * 1024
	EnvHeadroomBytes    int64 = 8 * 1024
	EnvMaxBytes               = EnvUpstreamMaxBytes - EnvHeadroomBytes

	CodeTooLarge = "worker_code_too_large"
	EnvTooLarge  = "worker_env_too_large"
)

// LimitError is safe to return across the control API boundary. It contains
// only aggregate byte counts, never module source, env keys, or env values.
type LimitError struct {
	Code   string `json:"code"`
	Actual int64  `json:"actual_bytes"`
	Max    int64  `json:"max_bytes"`
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("%s: %d bytes exceeds %d-byte limit", e.Code, e.Actual, e.Max)
}

// Module is one final WorkerCode module. Text and Data are separate because
// workerd measures UTF-8 source bytes and binary/data bytes directly.
type Module struct {
	Name string
	Text string
	Data []byte
}

// CodeInput is the explicit accounting list for a final WorkerCode.
// ModuleBytes supports callers that already know an immutable bundle's total
// payload bytes. Modules carries materialized modules when names and bodies
// must be counted individually.
type CodeInput struct {
	MainModule           string
	ModuleBytes          int64
	FixedInjectedBytes   int64
	Modules              []Module
	GeneratedWrapper     string
	FixedInjectedSources []string
}

// EstimateCode counts every supplied final WorkerCode component exactly once.
// It saturates rather than wrapping so an impossible size cannot bypass a
// security limit.
func EstimateCode(input CodeInput) int64 {
	total := int64(0)
	add := func(n int64) {
		if n <= 0 || total == math.MaxInt64 {
			return
		}
		if n > math.MaxInt64-total {
			total = math.MaxInt64
			return
		}
		total += n
	}

	add(int64(len(input.MainModule)))
	add(input.ModuleBytes)
	add(input.FixedInjectedBytes)
	add(int64(len(input.GeneratedWrapper)))
	for _, module := range input.Modules {
		add(int64(len(module.Name)))
		add(int64(len(module.Text)))
		add(int64(len(module.Data)))
	}
	for _, source := range input.FixedInjectedSources {
		add(int64(len(source)))
	}
	return total
}

func CheckCode(input CodeInput) error {
	actual := EstimateCode(input)
	if actual <= CodeMaxBytes {
		return nil
	}
	return &LimitError{Code: CodeTooLarge, Actual: actual, Max: CodeMaxBytes}
}

// EstimateEnv returns the UTF-8 JSON size plus the conservative V8 two-byte
// representation penalty used by workerd for every non-Latin-1 key/value
// string. JSON encoding first rejects cycles and unsupported values.
func EstimateEnv(value any) (int64, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return 0, fmt.Errorf("encode worker env: %w", err)
	}
	jsonBytes := encoded.Bytes()
	if len(jsonBytes) > 0 && jsonBytes[len(jsonBytes)-1] == '\n' {
		jsonBytes = jsonBytes[:len(jsonBytes)-1]
	}

	decoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return 0, fmt.Errorf("normalize worker env: %w", err)
	}
	jsonSize, err := jsonValueSize(normalized)
	if err != nil {
		return 0, err
	}
	return jsonSize + envStringPenalty(normalized), nil
}

func CheckEnv(value any) error {
	actual, err := EstimateEnv(value)
	if err != nil {
		return err
	}
	if actual <= EnvMaxBytes {
		return nil
	}
	return &LimitError{Code: EnvTooLarge, Actual: actual, Max: EnvMaxBytes}
}

func envStringPenalty(value any) int64 {
	switch value := value.(type) {
	case string:
		return twoByteStringPenalty(value)
	case []any:
		var total int64
		for _, item := range value {
			total += envStringPenalty(item)
		}
		return total
	case map[string]any:
		var total int64
		for key, item := range value {
			total += twoByteStringPenalty(key)
			total += envStringPenalty(item)
		}
		return total
	default:
		return 0
	}
}

func jsonValueSize(value any) (int64, error) {
	switch value := value.(type) {
	case nil:
		return 4, nil
	case bool:
		if value {
			return 4, nil
		}
		return 5, nil
	case json.Number:
		return int64(len(value.String())), nil
	case string:
		return quotedStringSize(value), nil
	case []any:
		total := int64(2)
		for i, item := range value {
			if i > 0 {
				total++
			}
			size, err := jsonValueSize(item)
			if err != nil {
				return 0, err
			}
			total += size
		}
		return total, nil
	case map[string]any:
		total := int64(2)
		first := true
		for key, item := range value {
			if !first {
				total++
			}
			first = false
			total += quotedStringSize(key) + 1
			size, err := jsonValueSize(item)
			if err != nil {
				return 0, err
			}
			total += size
		}
		return total, nil
	default:
		return 0, fmt.Errorf("normalized worker env contains unsupported %T", value)
	}
}

// quotedStringSize matches the UTF-8 byte length emitted by JSON.stringify.
// Unlike encoding/json it leaves U+2028 and U+2029 unescaped.
func quotedStringSize(value string) int64 {
	total := int64(2)
	for _, r := range value {
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			total += 2
		case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x0b, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
			0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f:
			total += 6
		default:
			total += int64(utf8.RuneLen(r))
		}
	}
	return total
}

func twoByteStringPenalty(value string) int64 {
	hasNonLatin1 := false
	utf16Units := 0
	for _, r := range value {
		if r >= 0x100 {
			hasNonLatin1 = true
		}
		utf16Units += utf16.RuneLen(r)
	}
	if !hasNonLatin1 {
		return 0
	}
	penalty := (2 * utf16Units) - len(value)
	if penalty <= 0 {
		return 0
	}
	return int64(penalty)
}
