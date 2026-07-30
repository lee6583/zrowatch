package httpserver

import (
	"strings"
	"testing"

	"transithub/backend/internal/modules/balance_control"
	"transithub/backend/internal/modules/my_sites"
)

func TestFormatAccountRenameSummary(t *testing.T) {
	message := formatAccountRenameSummary("王中王", []my_sites.AccountRenameResult{
		{GroupName: "王中王", OldName: "A-【王中王】-0.08x", NewName: "A-【王中王】-0.06x", Status: "updated"},
	})
	if !strings.Contains(message, "A-【王中王】-0.08x -> A-【王中王】-0.06x") {
		t.Fatalf("expected rename details in notification, got %q", message)
	}
}

func TestFormatBalanceLifecycleIncludesCountsAndAccountIDs(t *testing.T) {
	message := formatBalanceLifecycle("site", balance_control.Result{
		Transition: "paused", BalanceCNY: 1, Threshold: 10,
		Paused:  []balance_control.AccountAction{{AccountID: "acc-1", Name: "account", Status: "paused"}},
		Skipped: []balance_control.AccountAction{{AccountID: "acc-2", Status: "already_stopped"}},
		Failed:  []balance_control.AccountAction{{AccountID: "acc-3", Status: "put_failed"}},
	}, "")
	for _, expected := range []string{"成功 1，跳过 1，失败 1", "account(acc-1)", "acc-2", "acc-3"} {
		if !strings.Contains(message, expected) {
			t.Fatalf("expected %q in balance message, got %q", expected, message)
		}
	}
}

func TestFormatBalanceLifecycleIncludesCompleteProfitSummary(t *testing.T) {
	message := formatBalanceLifecycle("site", balance_control.Result{
		Transition: "paused", BalanceCNY: 0, Threshold: 10,
		Profit: &balance_control.ProfitReport{
			CycleFound: true, Complete: true, RechargeAmountCNY: 150,
			DownstreamIncomeCNY: 190, ProfitCNY: 40,
			Groups: []balance_control.ProfitGroupIncome{
				{GroupName: "codex_plus-福利", Amount: 120},
				{GroupName: "claude-福利", Amount: 70},
			},
		},
	}, "自定义余额提醒")
	for _, expected := range []string{"自定义余额提醒", "【本充值周期盈亏】", "充值合计：¥150.00", "下游用户扣费：¥190.00", "总盈利：¥40.00", "codex_plus-福利：¥120.00", "claude-福利：¥70.00"} {
		if !strings.Contains(message, expected) {
			t.Fatalf("expected %q in balance message, got %q", expected, message)
		}
	}
}

func TestFormatBalanceProfitSummaryDoesNotClaimIncompleteProfit(t *testing.T) {
	message := formatBalanceProfitSummary(&balance_control.ProfitReport{
		CycleFound: true, RechargeAmountCNY: 100, DownstreamIncomeCNY: 20,
		SuccessfulAccounts: 1, FailedAccounts: 1,
	})
	if !strings.Contains(message, "统计不完整") || strings.Contains(message, "总亏损：") || strings.Contains(message, "总盈利：") {
		t.Fatalf("incomplete report claimed a definitive result: %q", message)
	}
}

func TestFormatAccountRenameSummaryIgnoresOtherGroups(t *testing.T) {
	message := formatAccountRenameSummary("其他分组", []my_sites.AccountRenameResult{
		{GroupName: "王中王", Status: "failed"},
	})
	if message != "" {
		t.Fatalf("expected no summary for another group, got %q", message)
	}
}
