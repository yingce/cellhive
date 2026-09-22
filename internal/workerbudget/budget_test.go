package workerbudget

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
)

func TestCheckCodeBoundary(t *testing.T) {
	tests := []struct {
		name string
		n    int64
		ok   bool
	}{
		{name: "limit minus one", n: CodeMaxBytes - 1, ok: true},
		{name: "limit", n: CodeMaxBytes, ok: true},
		{name: "limit plus one", n: CodeMaxBytes + 1, ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckCode(CodeInput{ModuleBytes: tc.n})
			if (err == nil) != tc.ok {
				t.Fatalf("CheckCode(ModuleBytes=%d) error = %v, want ok=%v", tc.n, err, tc.ok)
			}
			if tc.ok {
				return
			}
			var limitErr *LimitError
			if !errors.As(err, &limitErr) {
				t.Fatalf("error type = %T, want *LimitError", err)
			}
			if limitErr.Code != CodeTooLarge || limitErr.Actual != tc.n || limitErr.Max != CodeMaxBytes {
				t.Fatalf("LimitError = %#v", limitErr)
			}
		})
	}
}

func TestEstimateCodeCountsEveryComponentOnce(t *testing.T) {
	input := CodeInput{
		MainModule:       "入口.js",
		ModuleBytes:      11,
		GeneratedWrapper: "wrap",
		Modules: []Module{
			{Name: "a.js", Text: "中文"},
			{Name: "data.bin", Data: []byte{0, 1, 2}},
		},
		FixedInjectedSources: []string{"fixed"},
	}

	// UTF-8 bytes: main 9 + aggregate 11 + wrapper 4 + names 4+8 + text 6
	// + data 3 + fixed source 5.
	const want int64 = 50
	if got := EstimateCode(input); got != want {
		t.Fatalf("EstimateCode() = %d, want %d", got, want)
	}
}

func TestEstimateEnvChargesJSONAndTwoByteStrings(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  int64
	}{
		{name: "empty object", value: map[string]any{}, want: 2},
		{name: "ASCII", value: map[string]any{"k": "aaaa"}, want: 12},
		// JSON is 14 bytes. Because one non-Latin-1 rune promotes the whole
		// five-code-unit string in V8, it adds 10-6=4 bytes of headroom.
		{name: "mixed Latin and non-Latin-1", value: map[string]any{"k": "aaaaĀ"}, want: 18},
		{name: "Chinese", value: map[string]any{"键": "中文中文"}, want: 22},
		{name: "emoji", value: map[string]any{"k": "😀"}, want: 12},
		{name: "mixed ASCII and emoji", value: map[string]any{"k": "aaaa😀"}, want: 20},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := EstimateEnv(tc.value)
			if err != nil {
				t.Fatalf("EstimateEnv() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("EstimateEnv() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestEstimateEnvRejectsUnsupportedAndCyclicValues(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["self"] = cyclic

	tests := []struct {
		name  string
		value any
	}{
		{name: "function", value: map[string]any{"bad": func() {}}},
		{name: "non finite number", value: map[string]any{"bad": math.Inf(1)}},
		{name: "cycle", value: cyclic},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := EstimateEnv(tc.value); err == nil {
				t.Fatal("EstimateEnv() error = nil, want rejection")
			}
		})
	}
}

func TestSharedGoldenVectors(t *testing.T) {
	type moduleVector struct {
		Name       string `json:"name"`
		Text       string `json:"text"`
		DataBase64 string `json:"dataBase64"`
	}
	type codeVector struct {
		Name                 string         `json:"name"`
		MainModule           string         `json:"mainModule"`
		ModuleBytes          int64          `json:"moduleBytes"`
		Modules              []moduleVector `json:"modules"`
		GeneratedWrapper     string         `json:"generatedWrapper"`
		FixedInjectedSources []string       `json:"fixedInjectedSources"`
		Want                 int64          `json:"want"`
		OK                   bool           `json:"ok"`
	}
	var vectors struct {
		Env []struct {
			Name      string `json:"name"`
			Value     any    `json:"value"`
			Want      int64  `json:"want"`
			OverLimit bool   `json:"overLimit"`
			Repeat    *struct {
				Key   string `json:"key"`
				Char  string `json:"char"`
				Count int    `json:"count"`
			} `json:"repeat"`
		} `json:"env"`
		Code []codeVector `json:"code"`
	}
	raw, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}

	for _, vector := range vectors.Env {
		t.Run("env/"+vector.Name, func(t *testing.T) {
			value := vector.Value
			if vector.Repeat != nil {
				value = map[string]any{vector.Repeat.Key: strings.Repeat(vector.Repeat.Char, vector.Repeat.Count)}
			}
			got, err := EstimateEnv(value)
			if err != nil {
				t.Fatal(err)
			}
			if got != vector.Want {
				t.Fatalf("EstimateEnv() = %d, want %d", got, vector.Want)
			}
			err = CheckEnv(value)
			if (err != nil) != vector.OverLimit {
				t.Fatalf("CheckEnv() error = %v, want overLimit=%v", err, vector.OverLimit)
			}
			if vector.OverLimit {
				var limitErr *LimitError
				if !errors.As(err, &limitErr) || limitErr.Code != EnvTooLarge ||
					limitErr.Actual != vector.Want || limitErr.Max != EnvMaxBytes {
					t.Fatalf("CheckEnv() error = %#v", err)
				}
			}
		})
	}
	for _, vector := range vectors.Code {
		t.Run("code/"+vector.Name, func(t *testing.T) {
			input := CodeInput{
				MainModule:           vector.MainModule,
				ModuleBytes:          vector.ModuleBytes,
				GeneratedWrapper:     vector.GeneratedWrapper,
				FixedInjectedSources: vector.FixedInjectedSources,
			}
			for _, rawModule := range vector.Modules {
				data, err := base64.StdEncoding.DecodeString(rawModule.DataBase64)
				if err != nil {
					t.Fatal(err)
				}
				input.Modules = append(input.Modules, Module{Name: rawModule.Name, Text: rawModule.Text, Data: data})
			}
			if got := EstimateCode(input); got != vector.Want {
				t.Fatalf("EstimateCode() = %d, want %d", got, vector.Want)
			}
			if gotOK := CheckCode(input) == nil; gotOK != vector.OK {
				t.Fatalf("CheckCode() ok = %v, want %v", gotOK, vector.OK)
			}
		})
	}
}
