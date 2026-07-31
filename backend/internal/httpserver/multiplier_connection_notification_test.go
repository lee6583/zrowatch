package httpserver

import (
	"testing"

	"transithub/backend/internal/modules/my_sites"
)

func TestIsUpstreamGroupMappedUsesActiveSiteAndGroupBinding(t *testing.T) {
	connections := []my_sites.RealConnection{
		{UpstreamSiteID: "site-1", UpstreamGroupID: "group-1", UpstreamGroupName: "codex低价", Status: my_sites.ConnectionStatusActive},
		{UpstreamSiteID: "site-1", UpstreamGroupID: "group-2", UpstreamGroupName: "其他", Status: "inactive"},
	}
	if !isUpstreamGroupMapped(connections, "site-1", "group-1", "codex低价") {
		t.Fatal("active matching binding should be classified as mapped")
	}
	if isUpstreamGroupMapped(connections, "site-1", "group-2", "其他") {
		t.Fatal("inactive binding should not be classified as mapped")
	}
	if isUpstreamGroupMapped(connections, "site-2", "group-1", "codex低价") {
		t.Fatal("binding from another site should not be classified as mapped")
	}
}

func TestIsUpstreamGroupMappedFallsBackToNameForLegacyBinding(t *testing.T) {
	connections := []my_sites.RealConnection{{
		UpstreamSiteID: "site-1", UpstreamGroupName: "codex低价", Status: my_sites.ConnectionStatusActive,
	}}
	if !isUpstreamGroupMapped(connections, "site-1", "new-id", "codex低价") {
		t.Fatal("legacy binding without a group id should match by name")
	}
}

func TestClassifyMultiplierChangeMessage(t *testing.T) {
	base := "【倍率变更】柠檬冰拿铁 的 codex低价 分组倍率已下降"
	if got := classifyMultiplierChangeMessage(base, true); got != "【已对接倍率变更】柠檬冰拿铁 的 codex低价 分组倍率已下降" {
		t.Fatalf("mapped message = %q", got)
	}
	if got := classifyMultiplierChangeMessage(base, false); got != "【未对接倍率变更】柠檬冰拿铁 的 codex低价 分组倍率已下降" {
		t.Fatalf("unmapped message = %q", got)
	}
	if got := classifyMultiplierChangeMessage("自定义模板", true); got != "【已对接倍率变更】自定义模板" {
		t.Fatalf("custom mapped message = %q", got)
	}
}
