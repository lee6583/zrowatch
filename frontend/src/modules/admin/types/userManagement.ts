export interface UserBalanceRule {
  warningEnabled: boolean
  warningThreshold: number | null
  autoRechargeEnabled: boolean
  autoRechargeThreshold: number | null
  autoRechargeAmount: number | null
  warningActive: boolean
  rechargePending: boolean
  lastCheckedAt?: string | null
  lastWarningAt?: string | null
  lastRechargeAt?: string | null
  lastErrorKey?: string | null
}

export interface ManagedSub2APIUser {
  id: string
  email: string
  username: string
  role: string
  status: string
  balance: number | null
  createdAt?: string | null
  rule?: UserBalanceRule | null
}

export interface UserBalanceRuleInput {
  warningEnabled: boolean
  warningThreshold: number | null
  autoRechargeEnabled: boolean
  autoRechargeThreshold: number | null
  autoRechargeAmount: number | null
}

export interface ManagedUsersQuery {
  page: number
  pageSize: number
  status: string
  role: string
  search: string
  sortBy: string
  sortOrder: string
  timezone: string
}

export interface ManagedUsersPage {
  items: ManagedSub2APIUser[]
  total: number
  page: number
  pageSize: number
  totalPages: number
}
