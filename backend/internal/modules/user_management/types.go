package user_management

import "time"

type requestError string

func (e requestError) Error() string { return string(e) }

const (
	ErrInvalidRequest   = requestError("admin.userManagement.errors.invalidRequest")
	ErrUpstreamAuth     = requestError("admin.userManagement.errors.upstreamAuth")
	ErrUpstreamRequest  = requestError("admin.userManagement.errors.upstreamRequest")
	ErrPersistence      = requestError("admin.userManagement.errors.persistence")
	ErrNoCurrentAccount = requestError("admin.adminAccounts.errors.noCurrentAccount")
)

type UserQuery struct {
	Page      int
	PageSize  int
	Status    string
	Role      string
	Search    string
	SortBy    string
	SortOrder string
	Timezone  string
}

type RuleInput struct {
	WarningEnabled        bool     `json:"warningEnabled"`
	WarningThreshold      *float64 `json:"warningThreshold"`
	AutoRechargeEnabled   bool     `json:"autoRechargeEnabled"`
	AutoRechargeThreshold *float64 `json:"autoRechargeThreshold"`
	AutoRechargeAmount    *float64 `json:"autoRechargeAmount"`
}

type Rule struct {
	UserID                string
	AdminAccountID        string
	UpstreamUserID        string
	Email                 string
	Username              string
	WarningEnabled        bool
	WarningThreshold      *float64
	AutoRechargeEnabled   bool
	AutoRechargeThreshold *float64
	AutoRechargeAmount    *float64
	WarningActive         bool
	RechargeLatched       bool
	RechargePending       bool
	RechargeEventID       string
	LastBalance           *float64
	LastCheckedAt         *time.Time
	LastWarningAt         *time.Time
	LastRechargeAt        *time.Time
	LastErrorKey          string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type RuleDTO struct {
	WarningEnabled        bool       `json:"warningEnabled"`
	WarningThreshold      *float64   `json:"warningThreshold"`
	AutoRechargeEnabled   bool       `json:"autoRechargeEnabled"`
	AutoRechargeThreshold *float64   `json:"autoRechargeThreshold"`
	AutoRechargeAmount    *float64   `json:"autoRechargeAmount"`
	WarningActive         bool       `json:"warningActive"`
	RechargePending       bool       `json:"rechargePending"`
	LastCheckedAt         *time.Time `json:"lastCheckedAt,omitempty"`
	LastWarningAt         *time.Time `json:"lastWarningAt,omitempty"`
	LastRechargeAt        *time.Time `json:"lastRechargeAt,omitempty"`
	LastErrorKey          string     `json:"lastErrorKey,omitempty"`
}

type UserDTO struct {
	ID        string     `json:"id"`
	Email     string     `json:"email"`
	Username  string     `json:"username"`
	Role      string     `json:"role"`
	Status    string     `json:"status"`
	Balance   *float64   `json:"balance"`
	CreatedAt *time.Time `json:"createdAt,omitempty"`
	Rule      *RuleDTO   `json:"rule,omitempty"`
}

type UsersPage struct {
	Items    []UserDTO `json:"items"`
	Total    int       `json:"total"`
	Page     int       `json:"page"`
	PageSize int       `json:"pageSize"`
	Pages    int       `json:"pages"`
}
