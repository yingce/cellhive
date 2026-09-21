package userruntime

import (
	"strings"
	"testing"
)

func TestEgressAllowListRendering(t *testing.T) {
	// 未配置 → 宽松回落（public+private）
	c := Config{}
	if got := c.EgressAllowList(); got != `"public", "private"` {
		t.Fatalf("empty EgressAllow = %q, want public+private", got)
	}
	// 配置段 → public + 段
	c2 := Config{EgressAllow: []string{"10.20.0.0/16"}}
	if got := c2.EgressAllowList(); got != `"public", "10.20.0.0/16"` {
		t.Fatalf("configured = %q", got)
	}
	// 模板渲染：EgressAllowList 填进 capnp 的 cap-egress 行
	rendered := strings.ReplaceAll(capnpTemplate, "{{.EgressAllowList}}", (Config{EgressAllow: []string{"10.20.0.0/16"}}).EgressAllowList())
	rendered = strings.ReplaceAll(rendered, "{{.OutboundAllowList}}", (Config{}).OutboundAllowList())
	if !strings.Contains(rendered, `(name = "cap-egress", network = ( allow = ["public", "10.20.0.0/16"] ))`) {
		t.Fatalf("cap-egress line not rendered as expected")
	}
	if strings.Contains(rendered, "{{.EgressAllowList}}") {
		t.Fatal("placeholder left unrendered")
	}
}
