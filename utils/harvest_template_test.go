package utils

import (
	"strings"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

func TestRenderTemplate_AllTokens(t *testing.T) {
	tmpl := `PORT: {{.PORT}}
SERVICE_CONTROL_URL: {{.SERVICE_CONTROL_URL}}
SERVICE_NAME: {{.SERVICE_NAME}}
POLLER_NAME: {{.POLLER_NAME}}
DATACENTER: {{.DATACENTER}}
NODE_IP: {{.NODE_IP}}
AUTH_STYLE: {{.AUTH_STYLE}}
USERNAME: {{.USERNAME}}
PASSWORD: {{.PASSWORD}}
PROJECT: {{.PROJECT}}`

	tokens := datamodel.HarvestConfig{
		PORT:                "1234",
		SERVICE_CONTROL_URL: "https://servicecontrol.example.com",
		SERVICE_NAME:        "svc-name",
		POLLER_NAME:         "poller-01",
		DATACENTER:          "us-east1",
		NODE_IP:             "1.2.3.4",
		AUTH_STYLE:          "basic_auth",
		USERNAME:            "admin",
		PASSWORD:            "secret",
		PROJECT:             "proj-123",
	}

	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "1234") || !strings.Contains(out, "poller-01") || !strings.Contains(out, "secret") {
		t.Errorf("token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_EmptyTemplate(t *testing.T) {
	out, err := RenderTemplate("", &datamodel.HarvestConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "" {
		t.Errorf("expected empty output, got %q", out)
	}
}

func TestRenderTemplate_DefaultValues(t *testing.T) {
	tmpl := `AUTH_STYLE: {{.AUTH_STYLE}}
USERNAME: {{.USERNAME}}
PASSWORD: {{.PASSWORD}}`
	tokens := datamodel.HarvestConfig{
		AUTH_STYLE: "", // Intentionally empty
		USERNAME:   "", // Intentionally empty
		PASSWORD:   "", // Intentionally empty
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should render empty values for missing tokens
	if !strings.Contains(out, "AUTH_STYLE: ") || !strings.Contains(out, "USERNAME: ") || !strings.Contains(out, "PASSWORD: ") {
		t.Errorf("expected empty values for missing tokens, got %q", out)
	}
}

func TestRenderTemplate_ExtraTokensInStruct(t *testing.T) {
	tmpl := `PORT: {{.PORT}}`
	tokens := datamodel.HarvestConfig{
		PORT:                "5555",
		SERVICE_CONTROL_URL: "should not matter",
		SERVICE_NAME:        "should not matter",
		POLLER_NAME:         "should not matter",
		DATACENTER:          "should not matter",
		NODE_IP:             "should not matter",
		AUTH_STYLE:          "should not matter",
		USERNAME:            "should not matter",
		PASSWORD:            "should not matter",
		PROJECT:             "should not matter",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "5555") {
		t.Errorf("token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_WhitespaceAndNewlines(t *testing.T) {
	tmpl := `PORT:    {{.PORT}}
SERVICE_NAME:   {{.SERVICE_NAME}}
`
	tokens := datamodel.HarvestConfig{
		PORT:         "8080",
		SERVICE_NAME: "svc",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "PORT:    8080") || !strings.Contains(out, "SERVICE_NAME:   svc") {
		t.Errorf("token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_UnicodeValues(t *testing.T) {
	tmpl := `USERNAME: {{.USERNAME}}\nPASSWORD: {{.PASSWORD}}`
	tokens := datamodel.HarvestConfig{
		USERNAME: "админ",
		PASSWORD: "пароль",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "админ") || !strings.Contains(out, "пароль") {
		t.Errorf("unicode token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithSpecialChars(t *testing.T) {
	tmpl := `PASSWORD: {{.PASSWORD}}`
	tokens := datamodel.HarvestConfig{
		PASSWORD: `p@$$w0rd!#%&*()_+-=`,
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "p@$$w0rd!#%&*()_+-=") {
		t.Errorf("special char token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithMultilineValue(t *testing.T) {
	tmpl := `PASSWORD: |\n  {{.PASSWORD}}`
	tokens := datamodel.HarvestConfig{
		PASSWORD: "line1\nline2\nline3",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "line1\nline2\nline3") {
		t.Errorf("multiline token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TemplateWithNoTokens(t *testing.T) {
	tmpl := `static: value\nfoo: bar`
	out, err := RenderTemplate(tmpl, &datamodel.HarvestConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "static: value") || !strings.Contains(out, "foo: bar") {
		t.Errorf("static template failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithSpacesInValue(t *testing.T) {
	tmpl := `USERNAME: {{.USERNAME}}`
	tokens := datamodel.HarvestConfig{
		USERNAME: "user name with spaces",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "user name with spaces") {
		t.Errorf("space token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithEmptyString(t *testing.T) {
	tmpl := `PROJECT: {{.PROJECT}}`
	tokens := datamodel.HarvestConfig{
		PROJECT: "",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "PROJECT: ") {
		t.Errorf("empty string token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithNumericValues(t *testing.T) {
	tmpl := `PORT: {{.PORT}}\nPROJECT: {{.PROJECT}}`
	tokens := datamodel.HarvestConfig{
		PORT:    "8081",
		PROJECT: "1234567890",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "PORT: 8081") || !strings.Contains(out, "PROJECT: 1234567890") {
		t.Errorf("numeric token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithLongString(t *testing.T) {
	longStr := strings.Repeat("x", 1000)
	tmpl := `PASSWORD: {{.PASSWORD}}`
	tokens := datamodel.HarvestConfig{
		PASSWORD: longStr,
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, longStr) {
		t.Errorf("long string token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithYAMLEscaping(t *testing.T) {
	tmpl := `PASSWORD: "{{.PASSWORD}}"`
	tokens := datamodel.HarvestConfig{
		PASSWORD: `pa:ss"word`,
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `PASSWORD: "pa:ss\"word"`) && !strings.Contains(out, `PASSWORD: "pa:ss"word"`) {
		t.Errorf("yaml escaping token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithTabAndControlChars(t *testing.T) {
	tmpl := `USERNAME: {{.USERNAME}}`
	tokens := datamodel.HarvestConfig{
		USERNAME: "user\tname\nwith\rcontrol",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "user\tname\nwith\rcontrol") {
		t.Errorf("control char token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithBracesInValue(t *testing.T) {
	tmpl := `PASSWORD: {{.PASSWORD}}`
	tokens := datamodel.HarvestConfig{
		PASSWORD: "{braces} and [brackets]",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "{braces} and [brackets]") {
		t.Errorf("braces token substitution failed: got %q", out)
	}
}

func TestRenderTemplate_TokenWithPercentAndDollar(t *testing.T) {
	tmpl := `PASSWORD: {{.PASSWORD}}`
	tokens := datamodel.HarvestConfig{
		PASSWORD: "%percent$money$",
	}
	out, err := RenderTemplate(tmpl, &tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "%percent$money$") {
		t.Errorf("percent/dollar token substitution failed: got %q", out)
	}
}
