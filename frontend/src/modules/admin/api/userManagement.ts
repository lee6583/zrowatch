import type { ManagedUsersPage, ManagedUsersQuery, UserBalanceRule, UserBalanceRuleInput } from '../types/userManagement'
import { authUnauthorizedErrorKey, getAccessToken, handleAuthExpired, isUnauthorizedApiResponse } from '@/modules/auth/api/auth'

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? '/api'
const endpoint = (path: string): string => `${apiBaseUrl.replace(/\/$/, '')}${path}`

const requestJson = async <T>(path: string, options: RequestInit = {}): Promise<T> => {
  const token = getAccessToken()
  let response: Response
  try {
    response = await fetch(endpoint(path), {
      ...options,
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(options.headers ?? {}),
      },
    })
  } catch {
    throw new Error('admin.userManagement.errors.network')
  }
  const text = await response.text()
  let payload = {} as T & { message?: string }
  try { payload = text ? JSON.parse(text) as T & { message?: string } : payload } catch { throw new Error('admin.userManagement.errors.request') }
  if (!response.ok) {
    if (isUnauthorizedApiResponse(response.status, payload)) {
      handleAuthExpired()
      throw new Error(authUnauthorizedErrorKey)
    }
    throw new Error(payload.message ?? 'admin.userManagement.errors.request')
  }
  return payload
}

export const listManagedUsers = async (query: ManagedUsersQuery): Promise<ManagedUsersPage> => {
  const params = new URLSearchParams({
    page: String(query.page), page_size: String(query.pageSize), status: query.status, role: query.role,
    search: query.search, sort_by: query.sortBy, sort_order: query.sortOrder, timezone: query.timezone,
  })
  const payload = await requestJson<{
    items?: ManagedUsersPage['items']; total?: number; page?: number; pageSize?: number; page_size?: number; pages?: number
  }>(`/user-management/users?${params.toString()}`)
  return {
    items: payload.items ?? [], total: payload.total ?? 0, page: payload.page ?? 1,
    pageSize: payload.pageSize ?? payload.page_size ?? query.pageSize, totalPages: Math.max(payload.pages ?? 1, 1),
  }
}

export const saveUserBalanceRule = async (userId: string, input: UserBalanceRuleInput): Promise<UserBalanceRule | null> => {
  const response = await requestJson<{ rule: UserBalanceRule | null }>(`/user-management/users/${encodeURIComponent(userId)}/rule`, {
    method: 'PUT', body: JSON.stringify(input),
  })
  return response.rule ?? null
}

export const deleteUserBalanceRule = async (userId: string): Promise<void> => {
  await requestJson<{ success: boolean }>(`/user-management/users/${encodeURIComponent(userId)}/rule`, { method: 'DELETE' })
}
