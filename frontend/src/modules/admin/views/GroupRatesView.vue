<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'
import { AlertCircle, ArrowUpDown, Check, History, KeyRound, Link2, Loader2, Megaphone, Pencil, RefreshCw, Search, ServerCog, Settings2, Sparkles, Trash2, X, Zap } from 'lucide-vue-next'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { Tooltip } from '@/components/ui/tooltip'
import { getMySiteMappingOptions, realConnect, realBind, listAdminResources, listUpstreamKeys, listRealConnections, realDisconnect, updateRealConnectionGroups } from '../api/mySites'
import { getDashboardAdminStatus } from '../api/dashboardAdmin'
import { getBoundDispatchAccounts, getGroupRateMonitorSettings, getGroupRateMonitorSummaries, probeGroupRateMonitor, saveGroupRateMonitorSettings, updateGroupRateMonitorCostGuard, updateTargetDispatch, updateTargetPriority } from '../api/connectionHealth'
import { listUpstreamSites, syncAllUpstreamSites } from '../api/upstream'
import { useGroupRates } from '../composables/useGroupRates'
import type { GroupRate, GroupRateHistoryRow } from '../types/groupRates'
import type { BoundDispatchAccountState, GroupRateMonitorGroupConfig, GroupRateMonitorSettings, GroupRateMonitorSummary, GroupRateProbeCycle } from '../types/connectionHealth'
import type { AdminResourceOption, ConnectionCapabilities, MySiteMapping, MySiteMappingOwnGroupOption, RealConnection, UpstreamKeyItem } from '../types/mySites'
import type { UpstreamSiteResponse } from '../types/upstream'
import { LEGACY_NEW_API_CHANNEL_SUGGESTIONS, NEW_API_CHANNEL_TYPES } from '../types/mySites'

const { t, locale } = useI18n()
const router = useRouter()

const {
  rates,
  history,
  total,
  page,
  pageSize,
  totalPages,
  types,
  platforms,
  typeFilter,
  platformFilter,
  statusFilter,
  sortMode,
  statusCounts,
  serverSupportsStatusFilters,
  search,
  isLoading,
  isHistoryLoading,
  isActionLoading,
  errorKey,
  historyErrorKey,
  loadRates,
  loadHistory,
  saveType,
  setSearch,
  setTypeFilter,
  setPlatformFilter,
  setStatusFilter,
  setSortMode,
  goToPage,
} = useGroupRates()

const selectedRate = ref<GroupRate | null>(null)
const isHistoryOpen = ref(false)
const editingRate = ref<GroupRate | null>(null)
const connectingRate = ref<GroupRate | null>(null)
const editTypeValue = ref('')
const connectOwnGroups = ref<string[]>([])
const connectMode = ref<'real' | 'bind'>('real')
const ownGroups = ref<MySiteMappingOwnGroupOption[]>([])
const mySiteMappings = ref<MySiteMapping[]>([])
const hasLoadedMappingOptions = ref(false)
const connectionCapabilities = ref<ConnectionCapabilities | null>(null)
const searchQuery = ref(search.value)
const realConnectionsData = ref<RealConnection[]>([])
const disconnectingRate = ref<GroupRate | null>(null)
const disconnectMode = ref<'unlink' | 'full'>('unlink')
const disconnectRemovePricing = ref(true)
const disconnectSelectedTargets = ref<string[]>([])
const isDisconnecting = ref(false)
const disconnectError = ref('')
const connectionEditingRate = ref<GroupRate | null>(null)
const editConnectionGroups = ref<string[]>([])
const isUpdatingConnectionGroups = ref(false)
const editConnectionError = ref('')
const isSyncingUpstream = ref(false)
const dispatchAccountsById = ref<Map<string, BoundDispatchAccountState>>(new Map())
const dispatchLoadingAccountIds = ref<Set<string>>(new Set())
const isLoadingDispatchAccounts = ref(false)
const dispatchErrorKey = ref('')
const priorityDraftByAccountId = ref<Map<string, string>>(new Map())
const priorityLoadingAccountIds = ref<Set<string>>(new Set())
const priorityErrorKey = ref('')
const upstreamSiteURLsById = ref<Map<string, string>>(new Map())
const groupRateHealthSummaries = ref<Map<string, GroupRateMonitorSummary>>(new Map())
const isLoadingHealthSummaries = ref(false)
const healthSummaryErrorKey = ref('')
const probingHealthGroups = ref<Set<string>>(new Set())
const healthSettingsSnapshot = ref<GroupRateMonitorSettings | null>(null)
const isHealthSettingsOpen = ref(false)
const isLoadingHealthSettings = ref(false)
const isLoadingHealthSettingsSnapshot = ref(false)
const isSavingHealthSettings = ref(false)
const isTogglingCostGuard = ref(false)
const healthSettingsErrorKey = ref('')
const healthSettingsDraft = ref<GroupRateMonitorSettings | null>(null)
const healthSettingsGroupType = ref('')
const flashingHealthGroups = ref<Set<string>>(new Set())
const isAnyDialogOpen = computed(() => Boolean(isHistoryOpen.value || editingRate.value || connectingRate.value || connectionEditingRate.value || disconnectingRate.value || isHealthSettingsOpen.value))
let previouslyFocusedElement: HTMLElement | null = null
let previousBodyOverflow = ''
let healthPollingTimer: ReturnType<typeof setInterval> | null = null
let isDispatchAccountsRefreshInFlight = false
const healthStatusFlashTimers = new Map<string, ReturnType<typeof setTimeout>>()
const pendingBalanceExhaustedFlashAccountIds = new Set<string>()
const balanceSuspendedErrorKey = 'admin.connectionHealth.errors.balanceSuspended'

const shouldShowPageError = (key: string | null | undefined): boolean => Boolean(key && key !== balanceSuspendedErrorKey)
const isBalanceSuspendedError = (error: unknown): boolean => error instanceof Error && error.message === balanceSuspendedErrorKey

const dialogFocusableSelector = [
  'button:not([disabled])',
  'a[href]',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

const handleDialogKeydown = (event: KeyboardEvent) => {
  const dialog = document.querySelector<HTMLElement>('[data-group-rates-dialog]')
  if (!dialog) return
  if (event.key === 'Escape') {
    event.preventDefault()
    if (isHealthSettingsOpen.value && !isSavingHealthSettings.value) closeHealthSettings()
    else if (disconnectingRate.value && !isDisconnecting.value) closeDisconnect()
    else if (connectionEditingRate.value && !isUpdatingConnectionGroups.value) closeConnectionEditor()
    else if (connectingRate.value && !isActionLoading.value) closeConnector()
    else if (editingRate.value && !isActionLoading.value) closeTypeEditor()
    else if (isHistoryOpen.value) closeHistory()
    return
  }
  if (event.key !== 'Tab') return
  const focusableElements = Array.from(dialog.querySelectorAll<HTMLElement>(dialogFocusableSelector))
  if (focusableElements.length === 0) return
  const first = focusableElements[0]
  const last = focusableElements[focusableElements.length - 1]
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault()
    last.focus()
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault()
    first.focus()
  }
}
const selectedGroupType = ref('')
const selectedChannelType = ref(0)
const adminPlatform = ref('')
const upstreamKeys = ref<UpstreamKeyItem[]>([])
const selectedKeyId = ref('')
const isLoadingKeys = ref(false)
const selectedAdminGroupId = ref('')
const adminResources = ref<AdminResourceOption[]>([])
const selectedAdminResourceId = ref('')
const isLoadingAdminResources = ref(false)
const addToPricingMapping = ref(true)
const connectOperationId = ref('')
let searchDebounceTimer: ReturnType<typeof setTimeout> | null = null

const editTypeOptions = computed(() => {
  const options = new Set(types.value)
  if (editingRate.value?.type) options.add(editingRate.value.type)
  return Array.from(options).sort((first, second) => first.localeCompare(second))
})
const mappedOwnGroupsForRate = (rate: GroupRate): string[] => (
  mySiteMappings.value
    .filter((mapping) => mapping.upstreamTargets.some((target) => target.siteId === rate.siteId && target.groupName === rate.groupName))
    .map((mapping) => mapping.ownGroup)
)

const ownGroupsByName = computed(() => new Map(
  ownGroups.value.map(group => [group.groupName, group]),
))

const ownGroupsById = computed(() => new Map(
  ownGroups.value.map(group => [String(group.id), group]),
))

const downstreamGroupsForRate = (rate: GroupRate): Array<{ key: string; name: string; multiplier: number | null }> => {
  const connections = realConnectionsForRate(rate)
  const seen = new Set<string>()
  if (connections.length > 0) {
    return connections.flatMap(connection => (connection.ownGroupIds ?? []).flatMap((groupId, index) => {
      const normalizedId = String(groupId).trim()
      const configuredName = connection.ownGroupNames?.[index]?.trim() ?? ''
      const group = ownGroupsById.value.get(normalizedId) ?? ownGroupsByName.value.get(configuredName)
      const name = configuredName || group?.groupName || normalizedId
      const key = normalizedId || name
      if (!key || seen.has(key)) return []
      seen.add(key)
      return [{
        key,
        name,
        multiplier: group?.multiplier ?? null,
      }]
    }))
  }

  return mappedOwnGroupsForRate(rate).flatMap((name) => {
    const normalizedName = name.trim()
    if (!normalizedName || seen.has(normalizedName)) return []
    seen.add(normalizedName)
    const group = ownGroupsByName.value.get(normalizedName)
    return [{
      key: group?.id ?? normalizedName,
      name: normalizedName,
      multiplier: group?.multiplier ?? null,
    }]
  })
}

const firstMappedOwnGroupForRate = (rate: GroupRate): string => mappedOwnGroupsForRate(rate)[0] ?? ''

const filteredOwnGroups = computed(() => {
  // new-api admin 不按渠道类型筛选，直接显示全部自有分组
  if (isAdminNewAPI.value) return ownGroups.value
  const upstreamType = (connectingRate.value?.type || selectedGroupType.value).toLowerCase()
  if (upstreamType) {
    return ownGroups.value.filter(g => g.platform.toLowerCase() === upstreamType)
  }
  return ownGroups.value
})

const realConnectionForRate = (rate: GroupRate): RealConnection | undefined =>
  realConnectionsData.value.find(c => (
    c.upstreamSiteId === rate.siteId &&
    (c.upstreamGroupId === rate.groupId || ((!c.upstreamGroupId || !rate.groupId) && c.upstreamGroupName === rate.groupName))
  ))

const realConnectionsForRate = (rate: GroupRate): RealConnection[] =>
  realConnectionsData.value.filter(c => (
    c.upstreamSiteId === rate.siteId &&
    (c.upstreamGroupId === rate.groupId || ((!c.upstreamGroupId || !rate.groupId) && c.upstreamGroupName === rate.groupName))
  ))

const isSub2APIPlatform = (platform: string | undefined): boolean => platform?.toLowerCase().includes('sub2') ?? false

const normalizedDispatchPriority = (account: BoundDispatchAccountState | undefined): number => {
  const value = account?.priority
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? Math.trunc(value) : 1
}

const accountIdForRate = (rate: GroupRate): string | null => {
  const connection = realConnectionForRate(rate)
  if (!connection) return null
  return String(connection.adminAccountId)
}

const mergeDispatchAccount = (account: BoundDispatchAccountState) => {
  const accountId = String(account.id)
  const nextAccounts = new Map(dispatchAccountsById.value)
  nextAccounts.set(accountId, account)
  dispatchAccountsById.value = nextAccounts

  const nextDrafts = new Map(priorityDraftByAccountId.value)
  nextDrafts.set(accountId, String(normalizedDispatchPriority(account)))
  priorityDraftByAccountId.value = nextDrafts
}

const dispatchAccountForRate = (rate: GroupRate): BoundDispatchAccountState | undefined => {
  const connection = realConnectionForRate(rate)
  if (!connection || !isSub2APIPlatform(connection.adminPlatform || adminPlatform.value)) return undefined
  return dispatchAccountsById.value.get(String(connection.adminAccountId))
}

const isBalanceSuspendedAccount = (account: BoundDispatchAccountState | undefined): boolean => account?.unavailableReason === 'balance_suspended'

const isBalanceSuspendedRate = (rate: GroupRate): boolean => (
  realConnectionsForRate(rate).some(connection => (
    isSub2APIPlatform(connection.adminPlatform || adminPlatform.value) &&
    isBalanceSuspendedAccount(dispatchAccountsById.value.get(String(connection.adminAccountId)))
  ))
)

const supportsDispatch = (rate: GroupRate): boolean => {
  const connection = realConnectionForRate(rate)
  return !rate.deleted && Boolean(connection) && isSub2APIPlatform(connection?.adminPlatform || adminPlatform.value)
}

const hasDispatchState = (rate: GroupRate): boolean => {
  const account = dispatchAccountForRate(rate)
  return account?.available === true && Boolean(account.targetId) && typeof account.schedulable === 'boolean'
}

const isDispatchEnabled = (account: BoundDispatchAccountState | undefined): boolean => {
  return account?.schedulable === true && !isBalanceSuspendedAccount(account)
}

const isDispatchEnabledForRate = (rate: GroupRate): boolean => isDispatchEnabled(dispatchAccountForRate(rate))

const isDispatchUpdating = (rate: GroupRate): boolean => {
  const connection = realConnectionForRate(rate)
  return connection ? dispatchLoadingAccountIds.value.has(String(connection.adminAccountId)) : false
}

const isDispatchUnavailable = (rate: GroupRate): boolean => (
  supportsDispatch(rate) && !isLoadingDispatchAccounts.value && !hasDispatchState(rate)
)

const isPriorityUpdating = (rate: GroupRate): boolean => {
  const accountId = accountIdForRate(rate)
  return accountId ? priorityLoadingAccountIds.value.has(accountId) : false
}

const isPriorityUnavailable = (rate: GroupRate): boolean => (
  supportsDispatch(rate) && !isLoadingDispatchAccounts.value && !hasDispatchState(rate)
)

const priorityStatusLabel = (rate: GroupRate): string => {
  if (isPriorityUpdating(rate)) return t('admin.groupRates.priority.updating')
  if (isLoadingDispatchAccounts.value) return t('admin.groupRates.priority.loading')
  if (isPriorityUnavailable(rate)) return t('admin.groupRates.priority.unavailable')
  return t('admin.groupRates.priority.edit')
}

const priorityValueForRate = (rate: GroupRate): string => {
  const account = dispatchAccountForRate(rate)
  if (!account) return String(1)
  const accountId = String(account.id)
  return priorityDraftByAccountId.value.get(accountId) ?? String(normalizedDispatchPriority(account))
}

const setPriorityDraft = (accountId: string, value: string) => {
  const nextDrafts = new Map(priorityDraftByAccountId.value)
  nextDrafts.set(accountId, value)
  priorityDraftByAccountId.value = nextDrafts
}

const resetPriorityDraft = (accountId: string, account?: BoundDispatchAccountState) => {
  setPriorityDraft(accountId, String(normalizedDispatchPriority(account)))
}

const dispatchActionLabel = (rate: GroupRate): string => t(
  isDispatchEnabledForRate(rate) ? 'admin.groupRates.dispatch.disableForRate' : 'admin.groupRates.dispatch.enableForRate',
  { site: rate.siteName, group: rate.groupName },
)

const dispatchStatusLabel = (rate: GroupRate): string => {
  if (isDispatchUpdating(rate)) return t('admin.groupRates.dispatch.updating')
  if (isLoadingDispatchAccounts.value) return t('admin.groupRates.dispatch.loading')
  if (isBalanceSuspendedRate(rate)) return t('admin.groupRates.health.status.exhausted')
  if (isDispatchUnavailable(rate)) {
    return t(dispatchAccountForRate(rate)?.unavailableReason === 'not_found'
      ? 'admin.groupRates.dispatch.accountMissing'
      : 'admin.groupRates.dispatch.unavailable')
  }
  return dispatchActionLabel(rate)
}

const isRealConnected = (rate: GroupRate): boolean => !!realConnectionForRate(rate)
const siteURLForRate = (rate: GroupRate): string => upstreamSiteURLsById.value.get(rate.siteId) ?? ''
const isPricingMapped = (rate: GroupRate): boolean => rate.pricingMapped ?? mappedOwnGroupsForRate(rate).length > 0
const disconnectConnection = computed(() => disconnectingRate.value ? realConnectionForRate(disconnectingRate.value) : undefined)
const disconnectConnections = computed(() => disconnectingRate.value
  ? realConnectionsData.value.filter(c => (
      c.upstreamSiteId === disconnectingRate.value?.siteId &&
      (c.upstreamGroupId === disconnectingRate.value?.groupId || ((!c.upstreamGroupId || !disconnectingRate.value?.groupId) && c.upstreamGroupName === disconnectingRate.value?.groupName))
    ))
  : [])

const intendedDownstreamGroupsForConnection = (connection: RealConnection): Array<{ id: string; name: string }> => {
  const result: Array<{ id: string; name: string }> = []
  const seen = new Set<string>()
  const appendGroups = (ids: string[] | undefined, names: string[] | undefined) => {
    for (const [index, rawId] of (ids ?? []).entries()) {
      const id = String(rawId).trim()
      if (!id || seen.has(id)) continue
      seen.add(id)
      result.push({ id, name: names?.[index]?.trim() || id })
    }
  }
  appendGroups(connection.ownGroupIds, connection.ownGroupNames)
  appendGroups(connection.costGuardPausedOwnGroupIds, connection.costGuardPausedOwnGroupNames)
  return result
}

const disconnectTargets = computed(() => disconnectConnections.value.flatMap((connection) => {
  return intendedDownstreamGroupsForConnection(connection).map(group => ({
    key: `${connection.id}:${group.id}`,
    connectionId: connection.id,
    groupId: group.id,
    name: group.name,
    accountName: connection.adminAccountName,
  }))
}))
const disconnectCanDeleteRemote = computed(() => disconnectConnections.value.some(connection => connection.canDeleteRemote !== false))
const disconnectTargetCount = computed(() => disconnectSelectedTargets.value.length)
const connectionBeingEdited = computed(() => connectionEditingRate.value ? realConnectionForRate(connectionEditingRate.value) : undefined)

const loadRealConnections = async () => {
  try {
    realConnectionsData.value = await listRealConnections()
    flushBalanceExhaustedFlashes()
  } catch {}
}

const loadAdminPlatform = async () => {
  try {
    const status = await getDashboardAdminStatus()
    adminPlatform.value = status.platform ?? ''
  } catch {
    adminPlatform.value = ''
  }
}

const updateUpstreamSiteURLs = (sites: UpstreamSiteResponse[]) => {
  const urls = new Map<string, string>()
  for (const site of sites) {
    const baseURL = site.baseUrl.trim()
    if (baseURL && /^https?:\/\//i.test(baseURL)) urls.set(site.id, baseURL)
  }
  upstreamSiteURLsById.value = urls
}

const loadUpstreamSiteURLs = async () => {
  try {
    updateUpstreamSiteURLs(await listUpstreamSites())
  } catch {
    // The rates table remains usable when the optional external link lookup fails.
  }
}

const rateHealthKey = (siteId: string, groupId: string | null | undefined, groupName: string): string => (
  `${siteId}|${groupId?.trim() ? `id:${groupId.trim()}` : `name:${groupName.trim()}`}`
)

const healthSummaryForRate = (rate: GroupRate): GroupRateMonitorSummary | undefined => (
  groupRateHealthSummaries.value.get(rateHealthKey(rate.siteId, rate.groupId, rate.groupName))
)

const healthHistorySize = 5

const healthSlotsForRate = (rate: GroupRate): Array<GroupRateProbeCycle | null> => {
  const events = healthSummaryForRate(rate)?.events ?? []
  return [
    ...Array.from({ length: Math.max(0, healthHistorySize - events.length) }, () => null),
    ...events.slice(-healthHistorySize),
  ]
}

const healthStatusLabel = (rate: GroupRate): string => {
  if (isBalanceSuspendedRate(rate)) return t('admin.groupRates.health.status.exhausted')
  const summary = healthSummaryForRate(rate)
  if (!summary || !summary.model) return t('admin.groupRates.health.status.unconfigured')
  if (summary.stale) return t('admin.groupRates.health.status.stale')
  return t(`admin.groupRates.health.status.${summary.status}`)
}

const healthStatusClasses = (rate: GroupRate): string => {
  if (isBalanceSuspendedRate(rate)) return 'text-red-500'
  const summary = healthSummaryForRate(rate)
  if (!summary || !summary.model || summary.stale || summary.status === 'unavailable' || summary.status === 'unconfigured') {
    return 'text-muted-foreground'
  }
  if (summary.status === 'healthy') return 'text-emerald-600 dark:text-emerald-400'
  if (summary.status === 'warning') return 'text-amber-600 dark:text-amber-400'
  return 'text-red-500'
}

const isHealthStatusFlashing = (rate: GroupRate): boolean => (
  flashingHealthGroups.value.has(rateHealthKey(rate.siteId, rate.groupId, rate.groupName))
)

const flashHealthStatus = (rate: GroupRate) => {
  const key = rateHealthKey(rate.siteId, rate.groupId, rate.groupName)
  const existingTimer = healthStatusFlashTimers.get(key)
  if (existingTimer) clearTimeout(existingTimer)
  flashingHealthGroups.value = new Set(flashingHealthGroups.value).add(key)
  healthStatusFlashTimers.set(key, setTimeout(() => {
    const next = new Set(flashingHealthGroups.value)
    next.delete(key)
    flashingHealthGroups.value = next
    healthStatusFlashTimers.delete(key)
  }, 900))
}

const flushBalanceExhaustedFlashes = () => {
  if (pendingBalanceExhaustedFlashAccountIds.size === 0) return
  for (const rate of rates.value) {
    const matchedAccountIds = realConnectionsForRate(rate)
      .map(connection => String(connection.adminAccountId))
      .filter(accountId => pendingBalanceExhaustedFlashAccountIds.has(accountId))
    if (matchedAccountIds.length === 0) continue
    flashHealthStatus(rate)
    for (const accountId of matchedAccountIds) pendingBalanceExhaustedFlashAccountIds.delete(accountId)
  }
}

const healthBarClasses = (event: GroupRateProbeCycle | null, stale: boolean, balanceSuspended = false): string => {
  if (balanceSuspended) return 'bg-red-500'
  if (!event) return 'bg-muted'
  const opacity = stale ? ' opacity-40' : ''
  if (event.status === 'healthy') return `bg-emerald-500${opacity}`
  if (event.status === 'warning') return `bg-amber-500${opacity}`
  if (event.status === 'unhealthy') return `bg-red-500${opacity}`
  return `bg-muted-foreground/35${opacity}`
}

const healthCycleTooltip = (event: GroupRateProbeCycle | null): string => {
  if (!event) return t('admin.groupRates.health.noResult')
  const lines = [
    formatProbeDateTime(event.createdAt),
    t(`admin.groupRates.health.trigger.${event.trigger === 'manual' ? 'manual' : 'scheduled'}`),
    t('admin.groupRates.health.tooltip.model', { model: event.model }),
  ]
  for (const detail of event.details) {
    const latency = detail.latencyMs == null ? t('admin.groupRates.common.placeholder') : `${detail.latencyMs} ms`
    const result = detail.available
      ? (detail.healthy ? t('admin.groupRates.health.result.success') : t(`admin.groupRates.health.result.${detail.errorKey ?? 'failed'}`))
      : t(`admin.groupRates.health.unavailable.${detail.unavailableReason ?? 'unknown'}`)
    lines.push(`${t('admin.groupRates.health.tooltip.upstreamGroup', { group: detail.accountName || event.upstreamGroupName })} · ${latency} · ${result}`)
  }
  return lines.join('\n')
}

const formatHealthSuccessRate = (rate: GroupRate): string => {
  const summary = healthSummaryForRate(rate)
  if (!summary || summary.events.length === 0) return '0%'
  return `${Math.round(summary.successRate)}%`
}

const loadHealthSummaries = async (showLoading = false) => {
  if (statusFilter.value !== 'mapped' || document.visibilityState === 'hidden') return
  if (showLoading) isLoadingHealthSummaries.value = true
  healthSummaryErrorKey.value = ''
  try {
    const summaries = await getGroupRateMonitorSummaries()
    const next = new Map<string, GroupRateMonitorSummary>()
    for (const summary of summaries) {
      next.set(rateHealthKey(summary.upstreamSiteId, summary.upstreamGroupId, summary.upstreamGroupName), summary)
    }
    groupRateHealthSummaries.value = next
  } catch (error) {
    healthSummaryErrorKey.value = error instanceof Error ? error.message : 'admin.groupRates.health.errors.loadFailed'
  } finally {
    if (showLoading) isLoadingHealthSummaries.value = false
  }
}

const stopHealthPolling = () => {
  if (healthPollingTimer) clearInterval(healthPollingTimer)
  healthPollingTimer = null
}

const startHealthPolling = () => {
  stopHealthPolling()
  if (statusFilter.value !== 'mapped' || document.visibilityState === 'hidden') return
  void Promise.all([loadHealthSummaries(true), loadRealConnections(), loadDispatchAccounts(false)])
  healthPollingTimer = setInterval(() => {
    void Promise.all([loadHealthSummaries(), loadRealConnections(), loadDispatchAccounts(false)])
  }, 10_000)
}

const handleVisibilityChange = () => {
  if (document.visibilityState === 'hidden') stopHealthPolling()
  else startHealthPolling()
}

const openHealthSettings = async () => {
  isHealthSettingsOpen.value = true
  isLoadingHealthSettings.value = true
  healthSettingsErrorKey.value = ''
  try {
    const settings = await loadHealthSettingsSnapshot(true)
    healthSettingsDraft.value = cloneHealthSettings(settings)
    healthSettingsGroupType.value = healthSettingsGroupTypes.value[0] ?? ''
  } catch (error) {
    healthSettingsErrorKey.value = error instanceof Error ? error.message : 'admin.groupRates.health.errors.settingsLoadFailed'
  } finally {
    isLoadingHealthSettings.value = false
  }
}

const closeHealthSettings = () => {
  isHealthSettingsOpen.value = false
  healthSettingsDraft.value = null
  healthSettingsGroupType.value = ''
  healthSettingsErrorKey.value = ''
}

const cloneHealthSettings = (settings: GroupRateMonitorSettings): GroupRateMonitorSettings => ({
  ...settings,
  typeDefaults: settings.typeDefaults.map(item => ({ ...item })),
  groups: settings.groups.map(group => ({ ...group })),
  restore: { ...settings.restore },
})

const buildHealthSettingsInput = (settings: GroupRateMonitorSettings) => ({
  enabled: settings.enabled,
  costGuardEnabled: settings.costGuardEnabled,
  probeIntervalSeconds: Number(settings.probeIntervalSeconds),
  failureThreshold: Number(settings.failureThreshold),
  defaultModel: settings.defaultModel.trim(),
  typeDefaults: settings.typeDefaults.map(item => ({
    groupType: item.groupType,
    enabled: item.enabled,
    probeIntervalSeconds: Number(item.probeIntervalSeconds),
    failureThreshold: Number(item.failureThreshold),
    model: item.model.trim(),
  })),
  overrides: settings.groups.map(group => ({
    upstreamSiteId: group.upstreamSiteId,
    upstreamGroupId: group.upstreamGroupId,
    upstreamGroupName: group.upstreamGroupName,
    enabled: group.enabled,
    model: group.model.trim(),
    probeIntervalSeconds: group.probeIntervalSeconds,
    failureThreshold: group.failureThreshold,
  })),
})

const loadHealthSettingsSnapshot = async (force = false): Promise<GroupRateMonitorSettings> => {
  if (healthSettingsSnapshot.value && !force) return healthSettingsSnapshot.value
  isLoadingHealthSettingsSnapshot.value = true
  try {
    const settings = await getGroupRateMonitorSettings()
    healthSettingsSnapshot.value = cloneHealthSettings(settings)
    return healthSettingsSnapshot.value
  } finally {
    isLoadingHealthSettingsSnapshot.value = false
  }
}

const saveHealthSettings = async () => {
  const draft = healthSettingsDraft.value
  if (!draft || isSavingHealthSettings.value) return
  isSavingHealthSettings.value = true
  healthSettingsErrorKey.value = ''
  try {
    const saved = await saveGroupRateMonitorSettings(buildHealthSettingsInput(draft))
    healthSettingsSnapshot.value = cloneHealthSettings(saved)
    healthSettingsDraft.value = cloneHealthSettings(saved)
    closeHealthSettings()
    await loadHealthSummaries(true)
  } catch (error) {
    healthSettingsErrorKey.value = error instanceof Error ? error.message : 'admin.groupRates.health.errors.settingsSaveFailed'
  } finally {
    isSavingHealthSettings.value = false
  }
}

const toggleCostGuardEnabled = async () => {
  if (isLoadingHealthSettingsSnapshot.value || isTogglingCostGuard.value) return
  isTogglingCostGuard.value = true
  healthSettingsErrorKey.value = ''
  healthSummaryErrorKey.value = ''
  try {
    const settings = await loadHealthSettingsSnapshot()
    const result = await updateGroupRateMonitorCostGuard(!settings.costGuardEnabled)
    healthSettingsSnapshot.value = cloneHealthSettings({
      ...settings,
      costGuardEnabled: result.enabled,
    })
    if (healthSettingsDraft.value) {
      healthSettingsDraft.value.costGuardEnabled = result.enabled
    }
  } catch (error) {
    const errorKey = error instanceof Error ? error.message : 'admin.groupRates.health.errors.settingsSaveFailed'
    healthSettingsErrorKey.value = errorKey
    healthSummaryErrorKey.value = errorKey
  } finally {
    isTogglingCostGuard.value = false
  }
}

const setGroupProbeInterval = (group: GroupRateMonitorGroupConfig, event: Event) => {
  const raw = (event.target as HTMLInputElement).value.trim()
  group.probeIntervalSeconds = raw === '' ? null : Number(raw)
}

const setGroupFailureThreshold = (group: GroupRateMonitorGroupConfig, event: Event) => {
  const raw = (event.target as HTMLInputElement).value.trim()
  group.failureThreshold = raw === '' ? null : Number(raw)
}

const healthSettingsGroupTypes = computed(() => {
  const groups = healthSettingsDraft.value?.groups ?? []
  return [...new Set(groups.map(group => group.groupType.trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b))
})

const filteredHealthSettingsGroups = computed(() => {
  const groups = healthSettingsDraft.value?.groups ?? []
  if (!healthSettingsGroupType.value) return []
  return groups.filter(group => group.groupType === healthSettingsGroupType.value)
})

const selectedHealthTypeDefaults = computed(() => (
  healthSettingsDraft.value?.typeDefaults.find(item => item.groupType === healthSettingsGroupType.value) ?? null
))

const inheritedModelForGroup = (group: GroupRateMonitorGroupConfig): string => {
  return healthSettingsDraft.value?.typeDefaults.find(item => item.groupType === group.groupType)?.model.trim() ?? ''
}

const inheritedIntervalForGroup = (group: GroupRateMonitorGroupConfig): number => (
  healthSettingsDraft.value?.typeDefaults.find(item => item.groupType === group.groupType)?.probeIntervalSeconds ?? 30
)

const inheritedFailureThresholdForGroup = (group: GroupRateMonitorGroupConfig): number => (
  healthSettingsDraft.value?.typeDefaults.find(item => item.groupType === group.groupType)?.failureThreshold ?? 2
)

const probeHealthRate = async (rate: GroupRate) => {
  const key = rateHealthKey(rate.siteId, rate.groupId, rate.groupName)
  if (probingHealthGroups.value.has(key)) return
  healthSummaryErrorKey.value = ''
  probingHealthGroups.value = new Set(probingHealthGroups.value).add(key)
  try {
    const response = await probeGroupRateMonitor({
      upstreamSiteId: rate.siteId,
      upstreamGroupId: rate.groupId ?? '',
      upstreamGroupName: rate.groupName,
    })
    const summaries = new Map(groupRateHealthSummaries.value)
    summaries.set(key, response.summary)
    groupRateHealthSummaries.value = summaries
    for (const account of response.dispatchAccounts) mergeDispatchAccount(account)
    flashHealthStatus(rate)
  } catch (error) {
    if (isBalanceSuspendedError(error)) {
      await loadDispatchAccounts()
      flashHealthStatus(rate)
    } else {
      healthSummaryErrorKey.value = error instanceof Error ? error.message : 'admin.groupRates.health.errors.probeFailed'
    }
  } finally {
    const probing = new Set(probingHealthGroups.value)
    probing.delete(key)
    probingHealthGroups.value = probing
  }
}

const isHealthProbeRunning = (rate: GroupRate): boolean => probingHealthGroups.value.has(rateHealthKey(rate.siteId, rate.groupId, rate.groupName))

const canProbeHealthRate = (rate: GroupRate): boolean => {
  const summary = healthSummaryForRate(rate)
  return Boolean(summary?.model && isRealConnected(rate))
}

const filteredRates = computed(() => {
  if (serverSupportsStatusFilters.value) return rates.value

  const filtered = rates.value.filter(rate => {
    const typeMatch = !typeFilter.value || rate.type === typeFilter.value
    const platformMatch = !platformFilter.value || rate.platform === platformFilter.value

    if (statusFilter.value === 'deleted') {
      return typeMatch && platformMatch && rate.deleted
    }

    if (rate.deleted) return false

    const mappedMatch = statusFilter.value === 'all' ||
      (statusFilter.value === 'mapped' && rate.mapped) ||
      (statusFilter.value === 'unmapped' && !rate.mapped)

    return typeMatch && platformMatch && mappedMatch
  })

  return [...filtered].sort((a, b) => {
    switch (sortMode.value) {
      case 'multiplierAsc':
        return (a.currentMultiplier ?? Infinity) - (b.currentMultiplier ?? Infinity)
      case 'multiplierDesc':
        return (b.currentMultiplier ?? -Infinity) - (a.currentMultiplier ?? -Infinity)
      case 'siteNameAsc':
        return a.siteName.localeCompare(b.siteName)
      case 'groupNameAsc':
        return a.groupName.localeCompare(b.groupName)
    }
  })
})
const canGoPrevious = computed(() => page.value > 1 && !isLoading.value)
const canGoNext = computed(() => page.value < totalPages.value && !isLoading.value)

watch(searchQuery, (value) => {
  if (searchDebounceTimer) clearTimeout(searchDebounceTimer)
  searchDebounceTimer = setTimeout(() => {
    searchDebounceTimer = null
    void setSearch(value)
  }, 300)
})

const submitSearch = () => {
  if (searchDebounceTimer) {
    clearTimeout(searchDebounceTimer)
    searchDebounceTimer = null
  }
  void setSearch(searchQuery.value)
}

watch(statusFilter, (status) => {
  if (status === 'mapped') void loadHealthSettingsSnapshot().catch(() => undefined)
  if (status === 'mapped') startHealthPolling()
  else stopHealthPolling()
})
const hasAnyRateData = computed(() => Object.values(statusCounts.value).some(count => count > 0))
const hasActiveRateFilters = computed(() => Boolean(
  searchQuery.value.trim() ||
  typeFilter.value ||
  platformFilter.value ||
  statusFilter.value !== 'all',
))

watch(isAnyDialogOpen, async (open) => {
  if (open) {
    previouslyFocusedElement = document.activeElement as HTMLElement | null
    previousBodyOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    await nextTick()
    document.querySelector<HTMLElement>('[data-group-rates-dialog] button:not([disabled]), [data-group-rates-dialog] input:not([disabled])')?.focus()
    return
  }
  document.body.style.overflow = previousBodyOverflow
  previouslyFocusedElement?.focus()
  previouslyFocusedElement = null
})

onMounted(() => {
  document.addEventListener('keydown', handleDialogKeydown)
	document.addEventListener('visibilitychange', handleVisibilityChange)
  void Promise.all([
    loadRates(),
    loadRealConnections(),
    loadAdminPlatform(),
    loadUpstreamSiteURLs(),
    loadDispatchAccounts(),
    loadMappingOptions().catch(() => undefined),
  ])
  if (statusFilter.value === 'mapped') void loadHealthSettingsSnapshot().catch(() => undefined)
	if (statusFilter.value === 'mapped') startHealthPolling()
})

onBeforeUnmount(() => {
  if (searchDebounceTimer) clearTimeout(searchDebounceTimer)
	for (const timer of healthStatusFlashTimers.values()) clearTimeout(timer)
	healthStatusFlashTimers.clear()
	stopHealthPolling()
  document.removeEventListener('keydown', handleDialogKeydown)
	document.removeEventListener('visibilitychange', handleVisibilityChange)
  document.body.style.overflow = previousBodyOverflow
})

const isAdminNewAPI = computed(() => adminPlatform.value === 'newapi')
const needsGroupTypeSelection = computed(() => (
  connectMode.value === 'real' &&
  (connectionCapabilities.value?.requiresGroupType ?? (!connectingRate.value?.type && !isAdminNewAPI.value))
) && !connectingRate.value?.type)
const needsChannelTypeSelection = computed(() => connectMode.value === 'real' && (connectionCapabilities.value?.requiresChannelType ?? isAdminNewAPI.value))

// new-api admin：根据自有分组类型过滤可选的渠道类型
// 分组类型已知时只显示对应渠道，未知时显示全部
const filteredChannelTypes = computed(() => {
  const groupType = (connectingRate.value?.type || '').toLowerCase()
  const available = connectionCapabilities.value?.channelTypes?.length
    ? connectionCapabilities.value.channelTypes
    : NEW_API_CHANNEL_TYPES
  const suggestedId = connectionCapabilities.value?.suggestedChannelTypeByGroup?.[groupType]
    ?? LEGACY_NEW_API_CHANNEL_SUGGESTIONS[groupType]
  if (suggestedId) {
    return available.filter(channelType => channelType.id === suggestedId)
  }
  return available
})

const canSubmitConnect = computed(() => {
  if (!connectingRate.value) return false
  if (connectMode.value === 'bind') {
    return Boolean(selectedKeyId.value && selectedAdminGroupId.value && selectedAdminResourceId.value)
  }
  if (connectOwnGroups.value.length === 0) return false
  // sub2api admin：分组类型未知时必须手动选择
  if (needsGroupTypeSelection.value && !selectedGroupType.value) return false
  // new-api admin：必须选择渠道类型
  if (needsChannelTypeSelection.value && !selectedChannelType.value) return false
  return true
})

const handleTypeChange = async (event: Event) => {
  const target = event.target as HTMLSelectElement
  await setTypeFilter(target.value)
}

const formatMultiplier = (value: number | null): string => {
  if (value === null || !Number.isFinite(value)) return t('admin.groupRates.common.placeholder')
  return t('admin.groupRates.format.multiplier', { value: Number(value.toFixed(4)).toString() })
}

const formatDelta = (delta: number | null): string => {
  if (delta === null || !Number.isFinite(delta)) return t('admin.groupRates.common.placeholder')

  const sign = delta > 0 ? '+' : ''
  const deltaValue = `${sign}${Number(delta.toFixed(4)).toString()}`
  return t('admin.groupRates.format.deltaMultiplier', { value: deltaValue })
}

const formatDateTime = (value: string | null): string => {
  if (!value) return t('admin.groupRates.common.placeholder')
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return t('admin.groupRates.common.placeholder')
  return new Intl.DateTimeFormat(locale.value, {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

const formatProbeDateTime = (value: string | null): string => {
  if (!value) return t('admin.groupRates.common.placeholder')
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return t('admin.groupRates.common.placeholder')
  return new Intl.DateTimeFormat(locale.value, {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(date)
}

const platformLabel = (platform: string | null): string => {
  if (!platform) return t('admin.groupRates.common.unknown')
  if (platform === 'newapi') return t('admin.groupRates.platforms.newapi')
  if (platform === 'sub2api') return t('admin.groupRates.platforms.sub2api')
  return platform
}

const typeLabel = (type: string | null): string => {
  if (!type) return t('admin.groupRates.common.unknown')
  return type
}

const platformClasses = (platform: string | null): string => {
  if (platform === 'newapi') return 'border-sky-400/30 bg-sky-500/10 text-sky-600 dark:text-sky-300'
  if (platform === 'sub2api') return 'border-violet-400/30 bg-violet-500/10 text-violet-600 dark:text-violet-300'
  return 'border-border/60 bg-surface-elevated text-muted-foreground'
}

const typeClasses = (type: string | null): string => {
  if (!type) return 'border-border/60 bg-surface-elevated text-muted-foreground'

  let hash = 0
  for (const char of type) {
    hash = (hash + char.charCodeAt(0)) % 4
  }

  return [
    'border-emerald-400/30 bg-emerald-500/10 text-emerald-600 dark:text-emerald-300',
    'border-amber-400/30 bg-amber-500/10 text-amber-600 dark:text-amber-300',
    'border-rose-400/30 bg-rose-500/10 text-rose-600 dark:text-rose-300',
    'border-cyan-400/30 bg-cyan-500/10 text-cyan-600 dark:text-cyan-300',
  ][hash]
}

const deltaClasses = (delta: number | null): string => {
  if (delta === null || !Number.isFinite(delta)) return 'bg-surface-elevated text-muted-foreground border-border/50'
  if (delta > 0) return 'bg-red-500/10 text-red-500 border-red-500/20'
  if (delta < 0) return 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20'
  return 'bg-primary/10 text-primary border-primary/20'
}

const historyActionLabel = (rate: GroupRate): string => (
  t('admin.groupRates.actions.viewHistoryForRate', {
    site: rate.siteName,
    group: rate.groupName,
    delta: formatDelta(rate.delta),
  })
)

const openHistory = async (rate: GroupRate) => {
  selectedRate.value = rate
  isHistoryOpen.value = true
  await loadHistory({
    siteId: rate.siteId,
    groupId: rate.groupId,
    groupName: rate.groupId || rate.groupName,
    platform: rate.platform,
  })
}

const closeHistory = () => {
  isHistoryOpen.value = false
  selectedRate.value = null
}

const openTypeEditor = (rate: GroupRate) => {
  editingRate.value = rate
  editTypeValue.value = rate.type ?? ''
}

const closeTypeEditor = () => {
  editingRate.value = null
  editTypeValue.value = ''
}

const openConnector = async (rate: GroupRate) => {
  connectingRate.value = rate
  connectOwnGroups.value = []
  connectMode.value = 'real'
  selectedGroupType.value = ''
  selectedChannelType.value = 0
  addToPricingMapping.value = true
  connectOperationId.value = globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(36).slice(2)}`
  await loadMySiteMappingData()
}

const isActiveResourceStatus = (status: string): boolean => ['1', 'active', 'enabled'].includes(status.toLowerCase())

const resourceStatusLabel = (status: string): string => (
  isActiveResourceStatus(status)
    ? t('admin.groupRates.connect.resourceActive')
    : t('admin.groupRates.connect.resourceInactive')
)

const adminResourceTypeLabel = (resource: AdminResourceOption): string => {
  if (adminPlatform.value === 'newapi') {
    const channelType = NEW_API_CHANNEL_TYPES.find(item => item.id === Number(resource.type))
    if (channelType) return channelType.name
  }
  return resource.platform || resource.type || t('admin.groupRates.common.unknown')
}

const closeConnector = () => {
  connectingRate.value = null
  connectOwnGroups.value = []
  connectMode.value = 'real'
  realConnectError.value = ''
  selectedGroupType.value = ''
  selectedChannelType.value = 0
  upstreamKeys.value = []
  selectedKeyId.value = ''
  isLoadingKeys.value = false
  selectedAdminGroupId.value = ''
  adminResources.value = []
  selectedAdminResourceId.value = ''
  isLoadingAdminResources.value = false
  addToPricingMapping.value = true
  connectOperationId.value = ''
}

const setConnectMode = async (mode: 'real' | 'bind') => {
  connectMode.value = mode
  connectOwnGroups.value = []
  selectedGroupType.value = ''
  selectedChannelType.value = 0
  selectedKeyId.value = ''
  selectedAdminGroupId.value = ''
  selectedAdminResourceId.value = ''
  adminResources.value = []
  realConnectError.value = ''
  if (mode === 'bind' && connectingRate.value) {
    await loadUpstreamKeys(connectingRate.value)
  }
}

const loadDispatchAccounts = async (showLoading = true) => {
  if (isDispatchAccountsRefreshInFlight) return
  isDispatchAccountsRefreshInFlight = true
  if (showLoading) isLoadingDispatchAccounts.value = true
  dispatchErrorKey.value = ''
  try {
    const states = await getBoundDispatchAccounts()
    const previousAccounts = dispatchAccountsById.value
    const previousDrafts = priorityDraftByAccountId.value
    const accounts = new Map<string, BoundDispatchAccountState>()
    const drafts = new Map<string, string>()
    for (const account of states) {
      const accountId = String(account.id)
      const currentAccount = dispatchAccountsById.value.get(accountId)
      const isLocallyUpdating = dispatchLoadingAccountIds.value.has(accountId) || priorityLoadingAccountIds.value.has(accountId)
      const resolvedAccount = isLocallyUpdating && currentAccount ? currentAccount : account
      accounts.set(accountId, resolvedAccount)

      const previousAccount = previousAccounts.get(accountId)
      const previousDraft = previousDrafts.get(accountId)
      const hasUnsavedDraft = !showLoading && previousDraft != null && previousAccount != null &&
        previousDraft.trim() !== String(normalizedDispatchPriority(previousAccount))
      drafts.set(accountId, hasUnsavedDraft ? previousDraft : String(normalizedDispatchPriority(resolvedAccount)))

      if (isBalanceSuspendedAccount(resolvedAccount) && !isBalanceSuspendedAccount(previousAccount)) {
        pendingBalanceExhaustedFlashAccountIds.add(accountId)
      } else if (!isBalanceSuspendedAccount(resolvedAccount)) {
        pendingBalanceExhaustedFlashAccountIds.delete(accountId)
      }
    }
    dispatchAccountsById.value = accounts
    priorityDraftByAccountId.value = drafts
    flushBalanceExhaustedFlashes()
  } catch {
    if (showLoading) {
      dispatchAccountsById.value = new Map()
      priorityDraftByAccountId.value = new Map()
    }
    dispatchErrorKey.value = 'admin.groupRates.dispatch.loadFailed'
  } finally {
    isDispatchAccountsRefreshInFlight = false
    if (showLoading) isLoadingDispatchAccounts.value = false
  }
}

const loadAdminResourcesForGroup = async (groupId: string) => {
  selectedAdminGroupId.value = groupId
  selectedAdminResourceId.value = ''
  adminResources.value = []
  if (!groupId) return
  isLoadingAdminResources.value = true
  try {
    adminResources.value = await listAdminResources(groupId)
  } catch {
    adminResources.value = []
    realConnectError.value = t('admin.groupRates.connect.adminResourcesFailed')
  } finally {
    isLoadingAdminResources.value = false
  }
}

const handleAdminGroupChange = (event: Event) => {
  void loadAdminResourcesForGroup((event.target as HTMLSelectElement).value)
}

const submitTypeEditor = async () => {
  if (!editingRate.value) return
  await saveType(editingRate.value, editTypeValue.value.trim())
  closeTypeEditor()
}

const loadMappingOptions = async (force = false) => {
  if (hasLoadedMappingOptions.value && !force) return
  const options = await getMySiteMappingOptions()
  ownGroups.value = options.ownGroups
  mySiteMappings.value = options.mappings ?? []
  connectionCapabilities.value = options.connectionCapabilities ?? null
  hasLoadedMappingOptions.value = true
}

const loadMySiteMappingData = async (force = false) => {
  isActionLoading.value = true
  try {
    await loadMappingOptions(force)
  } finally {
    isActionLoading.value = false
  }
}

const toggleOwnGroup = (groupId: string) => {
  const index = connectOwnGroups.value.indexOf(groupId)
  if (index === -1) {
    connectOwnGroups.value = [...connectOwnGroups.value, groupId]
  } else {
    connectOwnGroups.value = connectOwnGroups.value.filter(id => id !== groupId)
  }
}

const submitConnector = async () => {
  if (!connectingRate.value || !canSubmitConnect.value) return

  if (connectMode.value === 'bind') {
    await submitBind()
  } else {
    await submitRealConnect()
  }
}

const realConnectError = ref('')

const refreshAfterMutation = async () => {
  try {
    await Promise.all([loadRates(), loadRealConnections(), loadMySiteMappingData(true), loadDispatchAccounts()])
    if (statusFilter.value === 'mapped') await loadHealthSummaries(true)
  } catch {
    errorKey.value = 'admin.groupRates.errors.refreshFailed'
  }
}

const refreshGroupRates = async () => {
  if (isSyncingUpstream.value || isLoading.value) return
  isSyncingUpstream.value = true
  errorKey.value = null
  try {
    updateUpstreamSiteURLs(await syncAllUpstreamSites())
    await Promise.all([loadRates(), loadRealConnections(), loadMySiteMappingData(true), loadDispatchAccounts()])
    if (statusFilter.value === 'mapped') await loadHealthSummaries(true)
  } catch (error) {
    errorKey.value = error instanceof Error ? error.message : 'admin.groupRates.errors.refreshFailed'
  } finally {
    isSyncingUpstream.value = false
  }
}

const toggleDispatch = async (rate: GroupRate) => {
  const connection = realConnectionForRate(rate)
  const account = dispatchAccountForRate(rate)
  if (!connection || !account || !supportsDispatch(rate)) return
  if (isBalanceSuspendedRate(rate)) return

  const accountId = String(connection.adminAccountId)
  if (dispatchLoadingAccountIds.value.has(accountId)) return
  dispatchErrorKey.value = ''
  const previousAccount = account
  const enabled = !isDispatchEnabled(account)
  const optimisticAccount: BoundDispatchAccountState = {
    ...account,
    status: enabled ? 'active' : 'inactive',
    schedulable: enabled,
    priority: account.priority,
    available: true,
  }
  const optimisticAccounts = new Map(dispatchAccountsById.value)
  optimisticAccounts.set(accountId, optimisticAccount)
  dispatchAccountsById.value = optimisticAccounts
  dispatchLoadingAccountIds.value = new Set(dispatchLoadingAccountIds.value).add(accountId)
  try {
    const state = await updateTargetDispatch(account.targetId, enabled)
    const updatedAccounts = new Map(dispatchAccountsById.value)
    const latestAccount = dispatchAccountsById.value.get(accountId) ?? previousAccount
    updatedAccounts.set(accountId, {
      ...latestAccount,
      status: state.status,
      schedulable: state.schedulable,
      priority: state.priority ?? latestAccount.priority,
    })
    dispatchAccountsById.value = updatedAccounts
    resetPriorityDraft(accountId, updatedAccounts.get(accountId))
  } catch (error) {
    if (dispatchAccountsById.value.get(accountId) === optimisticAccount) {
      const rolledBackAccounts = new Map(dispatchAccountsById.value)
      rolledBackAccounts.set(accountId, previousAccount)
      dispatchAccountsById.value = rolledBackAccounts
    }
    if (isBalanceSuspendedError(error)) {
      await loadDispatchAccounts()
      flashHealthStatus(rate)
    } else {
      dispatchErrorKey.value = error instanceof Error ? error.message : 'admin.groupRates.dispatch.updateFailed'
    }
  } finally {
    const loadingIds = new Set(dispatchLoadingAccountIds.value)
    loadingIds.delete(accountId)
    dispatchLoadingAccountIds.value = loadingIds
  }
}

const commitPriority = async (rate: GroupRate) => {
  const connection = realConnectionForRate(rate)
  const account = dispatchAccountForRate(rate)
  if (!connection || !account || !supportsDispatch(rate) || !hasDispatchState(rate)) return

  const accountId = String(connection.adminAccountId)
  if (priorityLoadingAccountIds.value.has(accountId)) return

  const currentPriority = normalizedDispatchPriority(account)
  const rawValue = (priorityDraftByAccountId.value.get(accountId) ?? String(currentPriority)).trim()
  const parsedValue = Number(rawValue)
  if (!rawValue || !Number.isFinite(parsedValue) || !Number.isInteger(parsedValue) || parsedValue < 1 || parsedValue > 50000) {
    resetPriorityDraft(accountId, account)
    return
  }

  const nextPriority = Math.trunc(parsedValue)
  if (nextPriority === currentPriority) {
    resetPriorityDraft(accountId, account)
    return
  }

  priorityErrorKey.value = ''
  const previousAccount = account
  const optimisticAccount: BoundDispatchAccountState = {
    ...account,
    priority: nextPriority,
    available: true,
  }
  const optimisticAccounts = new Map(dispatchAccountsById.value)
  optimisticAccounts.set(accountId, optimisticAccount)
  dispatchAccountsById.value = optimisticAccounts
  setPriorityDraft(accountId, String(nextPriority))
  priorityLoadingAccountIds.value = new Set(priorityLoadingAccountIds.value).add(accountId)
  try {
    const state = await updateTargetPriority(account.targetId, nextPriority)
    const updatedAccounts = new Map(dispatchAccountsById.value)
    const latestAccount = dispatchAccountsById.value.get(accountId) ?? previousAccount
    const resolvedPriority = state.priority ?? nextPriority
    updatedAccounts.set(accountId, {
      ...latestAccount,
      priority: resolvedPriority,
    })
    dispatchAccountsById.value = updatedAccounts
    setPriorityDraft(accountId, String(resolvedPriority))
  } catch (error) {
    if (dispatchAccountsById.value.get(accountId) === optimisticAccount) {
      const rolledBackAccounts = new Map(dispatchAccountsById.value)
      rolledBackAccounts.set(accountId, previousAccount)
      dispatchAccountsById.value = rolledBackAccounts
    }
    setPriorityDraft(accountId, String(currentPriority))
    if (isBalanceSuspendedError(error)) {
      await loadDispatchAccounts()
      flashHealthStatus(rate)
    } else {
      priorityErrorKey.value = error instanceof Error ? error.message : 'admin.groupRates.priority.updateFailed'
    }
  } finally {
    const loadingIds = new Set(priorityLoadingAccountIds.value)
    loadingIds.delete(accountId)
    priorityLoadingAccountIds.value = loadingIds
  }
}

const handlePriorityInput = (rate: GroupRate, event: Event) => {
  const accountId = accountIdForRate(rate)
  if (!accountId) return
  setPriorityDraft(accountId, (event.target as HTMLInputElement).value)
}

const handlePlatformChange = async (event: Event) => {
  const target = event.target as HTMLSelectElement
  await setPlatformFilter(target.value)
}

const handleStatusChange = async (status: 'all' | 'mapped' | 'unmapped' | 'deleted') => {
  if (status === statusFilter.value || isLoading.value) return
  await setStatusFilter(status)
}

const handleSortChange = async (event: Event) => {
  const target = event.target as HTMLSelectElement
  await setSortMode(target.value as 'multiplierAsc' | 'multiplierDesc' | 'siteNameAsc' | 'groupNameAsc')
}

const submitRealConnect = async () => {
  if (!connectingRate.value || connectOwnGroups.value.length === 0) return
  realConnectError.value = ''
  isActionLoading.value = true
  const payload = {
    upstreamSiteId: connectingRate.value.siteId,
    upstreamGroupId: connectingRate.value.groupId ?? '',
    upstreamGroupName: connectingRate.value.groupName,
    groupType: selectedGroupType.value,
    channelType: selectedChannelType.value || undefined,
    ownGroupIds: connectOwnGroups.value,
    addToPricingMapping: addToPricingMapping.value,
    operationId: connectOperationId.value,
  }
  try {
    await realConnect(payload)
    closeConnector()
  } catch {
    realConnectError.value = t('admin.groupRates.connect.realFailed')
    isActionLoading.value = false
    return
  }

  await refreshAfterMutation()
  isActionLoading.value = false
}

const loadUpstreamKeys = async (rate: GroupRate) => {
  isLoadingKeys.value = true
  try {
    upstreamKeys.value = await listUpstreamKeys(rate.siteId, rate.groupId ?? '', rate.groupName)
  } catch {
    upstreamKeys.value = []
  } finally {
    isLoadingKeys.value = false
  }
}

const submitBind = async () => {
  if (!connectingRate.value || !selectedKeyId.value || !selectedAdminGroupId.value || !selectedAdminResourceId.value) return
  const selectedKey = upstreamKeys.value.find(k => k.id === selectedKeyId.value)
  if (!selectedKey) return
  realConnectError.value = ''
  isActionLoading.value = true
  try {
    await realBind({
      upstreamSiteId: connectingRate.value.siteId,
      upstreamGroupId: connectingRate.value.groupId ?? '',
      upstreamGroupName: connectingRate.value.groupName,
      upstreamKeyId: selectedKey.id,
      upstreamKey: selectedKey.key,
      ownGroupIds: [selectedAdminGroupId.value],
      groupType: selectedGroupType.value,
      adminGroupId: selectedAdminGroupId.value,
      adminResourceId: selectedAdminResourceId.value,
      addToPricingMapping: addToPricingMapping.value,
      operationId: connectOperationId.value,
    })
    closeConnector()
  } catch {
    realConnectError.value = t('admin.groupRates.connect.bindFailed')
    isActionLoading.value = false
    return
  }

  await refreshAfterMutation()
  isActionLoading.value = false
}

const openDisconnect = (rate: GroupRate) => {
  disconnectingRate.value = rate
  disconnectMode.value = 'unlink'
  disconnectRemovePricing.value = realConnectionForRate(rate)?.pricingMappingEnabled ?? Boolean(rate.pricingMapped)
  disconnectSelectedTargets.value = []
  disconnectError.value = ''
}

const closeDisconnect = () => {
  disconnectingRate.value = null
  disconnectMode.value = 'unlink'
  disconnectRemovePricing.value = true
  disconnectSelectedTargets.value = []
  disconnectError.value = ''
}

const submitDisconnect = async () => {
  if (!disconnectingRate.value) return
  const conn = realConnectionForRate(disconnectingRate.value)
  if (!conn) return

  isDisconnecting.value = true
  disconnectError.value = ''
  try {
    const selectedByConnection = new Map<string, string[]>()
    for (const target of disconnectTargets.value) {
      if (!disconnectSelectedTargets.value.includes(target.key)) continue
      const ids = selectedByConnection.get(target.connectionId) ?? []
      ids.push(target.groupId)
      selectedByConnection.set(target.connectionId, ids)
    }
    if (selectedByConnection.size === 0) {
      disconnectError.value = t('admin.groupRates.disconnect.selectTarget')
      isDisconnecting.value = false
      return
    }
    for (const [connectionId, ownGroupIds] of selectedByConnection) {
      await realDisconnect({
        connectionId,
        mode: disconnectMode.value,
        ownGroupIds,
        removePricingMapping: disconnectRemovePricing.value,
      })
    }
    closeDisconnect()
  } catch (error) {
    const errorKey = error instanceof Error && error.message.startsWith('admin.') ? error.message : 'admin.groupRates.disconnect.failed'
    disconnectError.value = t(errorKey)
    isDisconnecting.value = false
    return
  }

  await refreshAfterMutation()
  isDisconnecting.value = false
}

const openConnectionEditor = async (rate: GroupRate) => {
  const connection = realConnectionForRate(rate)
  if (!connection) return
  connectionEditingRate.value = rate
  editConnectionGroups.value = intendedDownstreamGroupsForConnection(connection).map(group => group.id)
  editConnectionError.value = ''
  isUpdatingConnectionGroups.value = true
  try {
    await loadMappingOptions(true)
  } catch (error) {
    const errorKey = error instanceof Error && error.message.startsWith('admin.') ? error.message : 'admin.groupRates.connectionEdit.loadFailed'
    editConnectionError.value = t(errorKey)
  } finally {
    isUpdatingConnectionGroups.value = false
  }
}

const closeConnectionEditor = () => {
  connectionEditingRate.value = null
  editConnectionGroups.value = []
  editConnectionError.value = ''
}

const toggleEditConnectionGroup = (groupId: string) => {
  editConnectionGroups.value = editConnectionGroups.value.includes(groupId)
    ? editConnectionGroups.value.filter(id => id !== groupId)
    : [...editConnectionGroups.value, groupId]
}

const submitConnectionEditor = async () => {
  const connection = connectionBeingEdited.value
  if (!connection) return
  isUpdatingConnectionGroups.value = true
  editConnectionError.value = ''
  try {
    await updateRealConnectionGroups({
      connectionId: connection.id,
      ownGroupIds: editConnectionGroups.value,
    })
    await refreshAfterMutation()
    closeConnectionEditor()
  } catch (error) {
    const errorKey = error instanceof Error && error.message.startsWith('admin.') ? error.message : 'admin.groupRates.connectionEdit.failed'
    editConnectionError.value = t(errorKey)
  } finally {
    isUpdatingConnectionGroups.value = false
  }
}

const historyTitle = computed(() => {
  if (!selectedRate.value) return t('admin.groupRates.history.title')
  return t('admin.groupRates.history.titleWithGroup', {
    site: selectedRate.value.siteName,
    group: selectedRate.value.groupName,
  })
})

const editTypeTitle = computed(() => {
  if (!editingRate.value) return t('admin.groupRates.edit.title')
  return t('admin.groupRates.edit.titleWithGroup', {
    site: editingRate.value.siteName,
    group: editingRate.value.groupName,
  })
})

const historyRowKey = (row: GroupRateHistoryRow, index: number): string => (
  `${row.siteId}-${row.groupId || row.groupName}-${row.platform ?? 'all'}-${row.createdAt ?? index}`
)

</script>

<template>
  <div class="flex min-h-[calc(100dvh-8rem)] flex-col space-y-6 lg:h-[calc(100dvh-8rem)]">
    <div class="flex max-w-full w-fit shrink-0 items-center gap-1 overflow-x-auto rounded-lg border border-border/50 bg-surface p-1" role="tablist" :aria-label="t('admin.menu.groupRates')">
      <button
        v-for="tab in (['all', 'mapped', 'unmapped', 'deleted'] as const)"
        :key="tab"
        type="button"
        role="tab"
        :aria-selected="statusFilter === tab"
        aria-controls="group-rates-panel"
        :class="[
          'shrink-0 rounded-md px-4 py-1.5 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary',
          statusFilter === tab
            ? 'bg-primary text-primary-foreground shadow-sm'
            : 'text-muted-foreground hover:text-foreground hover:bg-surface-elevated'
        ]"
        @click="handleStatusChange(tab)"
      >
        <span>{{ t(`admin.groupRates.tabs.${tab}`) }}</span>
        <span
          class="ml-2 rounded bg-background/60 px-1.5 py-0.5 text-[11px] tabular-nums"
          :class="statusFilter === tab ? 'text-primary-foreground' : 'text-muted-foreground'"
        >
          {{ statusCounts[tab] }}
        </span>
      </button>
    </div>

    <div class="flex shrink-0 flex-col gap-4 xl:flex-row xl:items-center">
      <div class="grid min-w-0 w-full flex-1 grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-[10rem_8rem_8rem_9.5rem] xl:gap-2">
        <div class="relative w-full">
          <Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <input
            v-model="searchQuery"
            name="groupRateSearch"
            type="text"
            :placeholder="t('admin.groupRates.filters.searchPlaceholder')"
            :aria-label="t('admin.groupRates.filters.searchPlaceholder')"
            autocomplete="off"
            spellcheck="false"
            class="h-11 w-full rounded-lg border border-border/50 bg-surface pl-10 pr-4 text-sm text-foreground outline-none transition-[color,background-color,border-color,box-shadow] placeholder:text-muted-foreground focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30"
            @keydown.enter.prevent="submitSearch"
          />
        </div>

        <div class="w-full">
          <Select v-model="typeFilter" @change="handleTypeChange">
            <option value="">{{ t('admin.groupRates.common.allTypes') }}</option>
            <option v-for="type in types" :key="type" :value="type">{{ typeLabel(type) }}</option>
          </Select>
        </div>

        <div class="w-full">
          <Select v-model="platformFilter" @change="handlePlatformChange">
            <option value="">{{ t('admin.groupRates.common.allPlatforms') }}</option>
            <option v-for="platform in platforms" :key="platform" :value="platform">{{ platformLabel(platform) }}</option>
          </Select>
        </div>

        <div class="relative w-full">
          <ArrowUpDown class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
          <Select v-model="sortMode" class="pl-9" @change="handleSortChange">
            <option value="multiplierAsc">{{ t('admin.groupRates.sort.multiplierAsc') }}</option>
            <option value="multiplierDesc">{{ t('admin.groupRates.sort.multiplierDesc') }}</option>
            <option value="siteNameAsc">{{ t('admin.groupRates.sort.siteNameAsc') }}</option>
            <option value="groupNameAsc">{{ t('admin.groupRates.sort.groupNameAsc') }}</option>
          </Select>
        </div>
      </div>

      <div :class="['grid w-full shrink-0 grid-cols-1 gap-2 sm:w-auto sm:self-end xl:self-auto', statusFilter === 'mapped' ? 'sm:grid-cols-4' : 'sm:grid-cols-2']">
        <button
          v-if="statusFilter === 'mapped'"
          type="button"
          role="switch"
          :aria-checked="healthSettingsSnapshot?.costGuardEnabled ?? false"
          :disabled="isLoadingHealthSettingsSnapshot || isTogglingCostGuard || isSavingHealthSettings"
          :class="[
            'inline-flex h-11 items-center justify-between gap-3 rounded-lg border px-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-2 focus-visible:ring-offset-background',
            (healthSettingsSnapshot?.costGuardEnabled ?? false)
              ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
              : 'border-border/60 bg-surface text-muted-foreground hover:border-primary/40 hover:text-foreground',
            isLoadingHealthSettingsSnapshot || isTogglingCostGuard || isSavingHealthSettings ? 'cursor-wait opacity-70' : 'cursor-pointer',
          ]"
          @click="toggleCostGuardEnabled"
        >
          <span class="whitespace-nowrap">{{ t('admin.groupRates.health.settings.costGuard') }}</span>
          <span
            :class="[
              'relative inline-flex h-6 w-11 shrink-0 items-center rounded-full border transition-colors',
              healthSettingsSnapshot?.costGuardEnabled ? 'border-emerald-500/40 bg-emerald-500' : 'border-border/70 bg-muted',
            ]"
          >
            <Loader2 v-if="isLoadingHealthSettingsSnapshot || isTogglingCostGuard" class="absolute left-1.5 h-3.5 w-3.5 animate-spin text-white" />
            <span
              v-else
              :class="[
                'h-4 w-4 rounded-full bg-white shadow-sm transition-transform',
                healthSettingsSnapshot?.costGuardEnabled ? 'translate-x-5' : 'translate-x-1',
              ]"
            />
          </span>
        </button>
        <Button v-if="statusFilter === 'mapped'" variant="secondary" class="h-11 gap-2" @click="openHealthSettings">
          <Settings2 class="h-4 w-4" />
          {{ t('admin.groupRates.health.settings.action') }}
        </Button>
        <Button variant="secondary" class="h-11 gap-2" @click="router.push('/admin/group-rate-campaigns?action=create')">
          <Megaphone class="h-4 w-4" />
          {{ t('admin.groupRates.actions.createCampaign') }}
        </Button>
        <Button class="h-11 gap-2 shadow-sm" :disabled="isLoading || isSyncingUpstream" @click="refreshGroupRates">
          <Loader2 v-if="isLoading || isSyncingUpstream" class="h-4 w-4 animate-spin" />
          <RefreshCw v-else class="h-4 w-4" />
          {{ t('admin.groupRates.actions.refresh') }}
        </Button>
      </div>
    </div>

    <div v-if="shouldShowPageError(errorKey)" class="flex items-start gap-3 rounded-2xl border border-warning/20 bg-warning/10 p-4 text-sm text-warning shrink-0">
      <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
      <span>{{ t(errorKey ?? '') }}</span>
    </div>

    <div v-if="shouldShowPageError(dispatchErrorKey)" class="flex shrink-0 items-start gap-3 rounded-lg border border-destructive/20 bg-destructive/10 p-4 text-sm text-destructive">
      <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
      <span>{{ t(dispatchErrorKey) }}</span>
    </div>

    <div v-if="shouldShowPageError(priorityErrorKey)" class="flex shrink-0 items-start gap-3 rounded-lg border border-destructive/20 bg-destructive/10 p-4 text-sm text-destructive">
      <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
      <span>{{ t(priorityErrorKey) }}</span>
    </div>

    <div v-if="statusFilter === 'mapped' && shouldShowPageError(healthSummaryErrorKey)" class="flex shrink-0 items-start gap-3 rounded-lg border border-warning/20 bg-warning/10 p-4 text-sm text-warning">
      <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
      <span>{{ t(healthSummaryErrorKey) }}</span>
    </div>

    <div id="group-rates-panel" class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-xl border border-border/50 bg-card shadow-sm" role="tabpanel">
      <div v-if="isLoading" class="flex flex-1 items-center justify-center text-muted-foreground">
        <Loader2 class="mr-2 h-5 w-5 animate-spin" />
        {{ t('admin.groupRates.status.loading') }}
      </div>

      <div v-else-if="filteredRates.length === 0" class="flex flex-1 flex-col items-center justify-center px-6 text-center">
        <div class="flex h-12 w-12 items-center justify-center rounded-2xl border border-border/50 bg-surface-elevated text-muted-foreground">
          <History class="h-5 w-5" />
        </div>
        <h3 class="mt-4 font-semibold text-foreground">{{ t(hasActiveRateFilters || hasAnyRateData ? 'admin.groupRates.empty.filteredTitle' : 'admin.groupRates.empty.title') }}</h3>
        <p class="mt-2 max-w-sm text-sm text-muted-foreground">{{ t(hasActiveRateFilters || hasAnyRateData ? 'admin.groupRates.empty.filteredDescription' : 'admin.groupRates.empty.description') }}</p>
      </div>

      <div v-else class="flex-1 overflow-auto">
        <table :class="['relative w-full text-sm', statusFilter === 'mapped' ? 'min-w-[1290px]' : 'min-w-[1080px]']">
          <thead class="sticky top-0 z-10 border-b border-border/50 bg-surface-elevated/90 backdrop-blur-sm">
            <tr>
              <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.siteName') }}</th>
              <th class="px-6 py-3 text-center font-medium text-muted-foreground">
                {{ t(statusFilter === 'mapped' ? 'admin.groupRates.fields.upstreamGroup' : 'admin.groupRates.fields.groupName') }}
              </th>
              <th v-if="statusFilter === 'mapped'" class="px-6 py-3 text-center font-medium text-muted-foreground">
                {{ t('admin.groupRates.fields.downstreamGroup') }}
              </th>
              <th v-if="statusFilter !== 'mapped'" class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.type') }}</th>
              <th v-if="statusFilter !== 'mapped'" class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.platform') }}</th>
              <th v-if="statusFilter !== 'mapped'" class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.effectiveMultiplier') }}</th>
              <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.delta') }}</th>
              <th v-if="statusFilter === 'mapped'" class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.health') }}</th>
              <th class="px-6 py-3 text-center font-medium text-muted-foreground">
                {{ t(statusFilter === 'mapped' ? 'admin.groupRates.fields.latestProbe' : 'admin.groupRates.fields.updatedAt') }}
              </th>
              <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.dispatch') }}</th>
              <th v-if="statusFilter === 'mapped'" class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.priority') }}</th>
              <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.actions') }}</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-border/50">
            <tr v-for="rate in filteredRates" :key="`${rate.siteId}-${rate.groupName}-${rate.platform ?? 'all'}`" class="transition-colors hover:bg-surface/30">
              <td class="px-4 py-2.5">
                <a
                  v-if="statusFilter === 'mapped' && siteURLForRate(rate)"
                  :href="siteURLForRate(rate)"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="inline-flex items-center gap-1.5 font-medium text-foreground transition-colors hover:text-primary hover:underline focus-visible:rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary"
                  :title="siteURLForRate(rate)"
                >
                  <span>{{ rate.siteName }}</span>
                </a>
                <div v-else class="font-medium text-foreground">{{ rate.siteName }}</div>
              </td>
              <td class="px-4 py-2.5">
                <div class="flex items-center justify-center gap-1.5">
                  <span class="font-medium text-foreground">{{ rate.groupName }}</span>
                  <span v-if="statusFilter === 'mapped'" class="shrink-0 font-semibold tabular-nums text-foreground">
                    {{ formatMultiplier(rate.currentMultiplier) }}
                  </span>
                  <span v-if="rate.deleted" class="inline-flex rounded-md border border-red-500/20 bg-red-500/10 px-1.5 py-0.5 text-[10px] font-semibold text-red-500">{{ t('admin.groupRates.status.deleted') }}</span>
                </div>
              </td>
              <td v-if="statusFilter === 'mapped'" class="px-4 py-2.5">
                <div v-if="downstreamGroupsForRate(rate).length" class="flex flex-col items-center gap-1.5">
                  <div
                    v-for="group in downstreamGroupsForRate(rate)"
                    :key="group.key"
                    class="flex items-center justify-center gap-1.5 whitespace-nowrap text-foreground"
                  >
                    <span class="font-medium">{{ group.name }}</span>
                    <span class="font-semibold tabular-nums">{{ formatMultiplier(group.multiplier) }}</span>
                  </div>
                </div>
                <span v-else class="text-muted-foreground">{{ t('admin.groupRates.common.placeholder') }}</span>
              </td>
              <td v-if="statusFilter !== 'mapped'" class="px-4 py-2.5">
                <span :class="['inline-flex rounded-md border px-2 py-1 text-xs font-semibold uppercase tracking-wider', typeClasses(rate.type)]">
                  {{ typeLabel(rate.type) }}
                </span>
              </td>
              <td v-if="statusFilter !== 'mapped'" class="px-4 py-2.5">
                <span :class="['inline-flex rounded-md border px-2 py-1 text-xs font-semibold uppercase tracking-wider', platformClasses(rate.platform)]">
                  {{ platformLabel(rate.platform) }}
                </span>
              </td>
              <td v-if="statusFilter !== 'mapped'" class="px-4 py-2.5 tabular-nums">
                <div class="font-semibold text-foreground">{{ formatMultiplier(rate.currentMultiplier) }}</div>
                <div v-if="rate.upstreamMultiplier != null" class="mt-0.5 text-[11px] text-muted-foreground">
                  {{ t('admin.groupRates.fields.multiplierFormula', {
                    upstream: formatMultiplier(rate.upstreamMultiplier),
                    recharge: Number((rate.rechargeRate ?? 1).toFixed(4)).toString(),
                  }) }}
                </div>
              </td>
              <td class="px-4 py-2.5">
                <button
                  type="button"
                  :class="[
                    'inline-flex rounded-md border px-2.5 py-1 text-xs font-semibold transition-all hover:-translate-y-px hover:shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-2 focus-visible:ring-offset-background',
                    deltaClasses(rate.delta),
                  ]"
                  :title="historyActionLabel(rate)"
                  :aria-label="historyActionLabel(rate)"
                  @click="openHistory(rate)"
                >
                  {{ formatDelta(rate.delta) }}
                </button>
              </td>
              <td v-if="statusFilter === 'mapped'" class="px-4 py-2.5 text-center">
                <div class="mx-auto flex min-w-[260px] items-center justify-center gap-3">
                  <span
                    :class="[
                      'w-[68px] whitespace-nowrap rounded-sm text-right text-xs font-semibold transition-all duration-150',
                      healthStatusClasses(rate),
                      isHealthStatusFlashing(rate) ? 'animate-pulse ring-2 ring-current/40 ring-offset-2 ring-offset-background' : '',
                    ]"
                  >
                    {{ healthStatusLabel(rate) }}
                  </span>
                  <div :class="['flex h-6 items-center gap-1', healthSummaryForRate(rate)?.stale ? 'opacity-80' : '']">
                    <Tooltip
                      v-for="(event, index) in healthSlotsForRate(rate)"
                      :key="event?.id ?? `empty-${index}`"
                      :text="healthCycleTooltip(event)"
                      wide
                    >
                      <span
                        :class="['block h-5 w-2.5 rounded-[3px] transition-colors', healthBarClasses(event, healthSummaryForRate(rate)?.stale ?? false, isBalanceSuspendedRate(rate))]"
                      />
                    </Tooltip>
                  </div>
                  <span class="w-9 text-left text-xs font-semibold tabular-nums text-muted-foreground">
                    {{ formatHealthSuccessRate(rate) }}
                  </span>
                </div>
              </td>
              <td class="px-4 py-2.5 text-muted-foreground tabular-nums">
                {{ formatDateTime(statusFilter === 'mapped' ? (healthSummaryForRate(rate)?.latestProbeAt ?? null) : rate.updatedAt) }}
              </td>
              <td class="px-4 py-2.5 text-center">
                <button
                  v-if="supportsDispatch(rate)"
                  type="button"
                  role="switch"
                  :aria-checked="isDispatchEnabledForRate(rate)"
                  :aria-label="dispatchStatusLabel(rate)"
                  :title="dispatchStatusLabel(rate)"
                  :disabled="isDispatchUpdating(rate) || isLoadingDispatchAccounts || !hasDispatchState(rate) || isBalanceSuspendedRate(rate)"
                  :class="[
                    'relative inline-flex h-6 w-11 shrink-0 items-center rounded-full border transition-colors duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-2 focus-visible:ring-offset-background',
                    isDispatchUpdating(rate) || isLoadingDispatchAccounts
                      ? 'cursor-wait opacity-70'
                      : isDispatchUnavailable(rate) || isBalanceSuspendedRate(rate)
                        ? 'cursor-not-allowed opacity-50'
                        : 'cursor-pointer',
                    isDispatchEnabledForRate(rate)
                      ? 'border-emerald-500/40 bg-emerald-500'
                      : 'border-border/70 bg-muted hover:bg-muted/80',
                  ]"
                  @click="toggleDispatch(rate)"
                >
                  <span
                    :class="[
                      'flex h-4 w-4 items-center justify-center rounded-full bg-white text-muted-foreground shadow-sm transition-transform duration-200',
                      isDispatchEnabledForRate(rate) ? 'translate-x-5' : 'translate-x-1',
                    ]"
                  >
                    <Loader2 v-if="isDispatchUpdating(rate) || isLoadingDispatchAccounts" class="h-3 w-3 animate-spin" />
                  </span>
                </button>
                <span v-else class="text-muted-foreground">{{ t('admin.groupRates.common.placeholder') }}</span>
              </td>
              <td v-if="statusFilter === 'mapped'" class="px-4 py-2.5 text-center">
                <div v-if="supportsDispatch(rate) && hasDispatchState(rate)" class="relative mx-auto w-[96px]">
                  <input
                    :value="priorityValueForRate(rate)"
                    type="number"
                    min="1"
                    max="50000"
                    step="1"
                    :disabled="isPriorityUpdating(rate) || isLoadingDispatchAccounts"
                    :aria-label="priorityStatusLabel(rate)"
                    :title="priorityStatusLabel(rate)"
                    class="h-8 w-full rounded-md border border-border/70 bg-surface px-3 pr-8 text-center text-sm font-semibold tabular-nums text-foreground outline-none transition-[color,background-color,border-color,box-shadow] placeholder:text-muted-foreground focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30 disabled:cursor-not-allowed disabled:opacity-60"
                    @input="handlePriorityInput(rate, $event)"
                    @change="commitPriority(rate)"
                    @blur="commitPriority(rate)"
                    @keydown.enter.prevent="commitPriority(rate)"
                  />
                  <Loader2 v-if="isPriorityUpdating(rate) || isLoadingDispatchAccounts" class="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 animate-spin text-muted-foreground" />
                </div>
                <span v-else class="text-muted-foreground">{{ t('admin.groupRates.common.placeholder') }}</span>
              </td>
              <td class="px-4 py-2.5 text-center">
                <div v-if="!rate.deleted" class="flex justify-center gap-2">
                  <Tooltip v-if="isRealConnected(rate)" :text="canProbeHealthRate(rate) ? t('admin.groupRates.health.probe.action') : t('admin.groupRates.health.probe.disabled')">
                    <button
                      type="button"
                      class="flex h-8 w-8 items-center justify-center rounded-md border border-amber-500/30 bg-amber-500/10 text-amber-600 transition-colors hover:border-amber-500/50 hover:bg-amber-500/15 disabled:cursor-not-allowed disabled:opacity-45 dark:text-amber-400"
                      :aria-label="t('admin.groupRates.health.probe.action')"
                      :disabled="!canProbeHealthRate(rate) || isHealthProbeRunning(rate)"
                      @click="probeHealthRate(rate)"
                    >
                      <Loader2 v-if="isHealthProbeRunning(rate)" class="h-4 w-4 animate-spin" />
                      <Zap v-else class="h-4 w-4" />
                    </button>
                  </Tooltip>
                  <button
                    v-if="isRealConnected(rate)"
                    type="button"
                    class="flex h-8 w-8 items-center justify-center rounded-md border border-border/60 bg-surface text-muted-foreground transition-colors hover:border-primary/40 hover:bg-primary/10 hover:text-primary disabled:opacity-50"
                    :title="t('admin.groupRates.connectionEdit.action')"
                    :aria-label="t('admin.groupRates.connectionEdit.action')"
                    :disabled="isActionLoading || isUpdatingConnectionGroups"
                    @click="openConnectionEditor(rate)"
                  >
                    <Pencil class="h-4 w-4" />
                  </button>
                  <button
                    v-if="isRealConnected(rate)"
                    type="button"
                    class="flex h-8 w-8 items-center justify-center rounded-md border border-red-500/30 bg-red-500/10 text-red-500 transition-colors hover:border-red-500/50 hover:bg-red-500/15 disabled:opacity-50"
                    :title="t('admin.groupRates.disconnect.action')"
                    :aria-label="t('admin.groupRates.disconnect.action')"
                    :disabled="isActionLoading || isDisconnecting"
                    @click="openDisconnect(rate)"
                  >
                    <Trash2 class="h-4 w-4" />
                  </button>
                  <Button
                    v-else
                    variant="secondary"
                    size="sm"
                    class="gap-1.5 text-primary hover:text-primary"
                    :disabled="isActionLoading"
                    @click="openConnector(rate)"
                  >
                    <Link2 class="h-3.5 w-3.5" />
                    {{ t('admin.groupRates.actions.connect') }}
                  </Button>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="flex flex-col gap-3 border-t border-border/50 bg-surface-elevated/30 px-4 py-4 text-sm text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
        <div class="flex flex-wrap items-center gap-x-4 gap-y-1">
          <span>{{ t('admin.groupRates.pagination.total', { total }) }}</span>
          <span>{{ t('admin.groupRates.pagination.pageSize', { pageSize }) }}</span>
          <span>{{ t('admin.groupRates.pagination.currentPage', { page, totalPages }) }}</span>
        </div>

        <div class="flex items-center gap-2">
          <Button variant="secondary" size="sm" :disabled="!canGoPrevious" @click="goToPage(page - 1)">
            {{ t('admin.groupRates.pagination.previous') }}
          </Button>
          <Button variant="secondary" size="sm" :disabled="!canGoNext" @click="goToPage(page + 1)">
            {{ t('admin.groupRates.pagination.next') }}
          </Button>
        </div>
      </div>
    </div>

    <div v-if="isHealthSettingsOpen" class="fixed inset-0 z-50 flex items-center justify-center bg-background/80 p-4 backdrop-blur-sm">
      <div data-group-rates-dialog role="dialog" aria-modal="true" aria-labelledby="group-rate-health-settings-title" tabindex="-1" class="flex max-h-[calc(100dvh-2rem)] w-full max-w-5xl flex-col overflow-hidden rounded-xl border border-border/50 bg-card shadow-xl">
        <div class="flex items-start justify-between gap-4 border-b border-border/50 p-6">
          <div>
            <h2 id="group-rate-health-settings-title" class="text-xl font-semibold text-foreground">{{ t('admin.groupRates.health.settings.title') }}</h2>
          </div>
          <button class="rounded-lg p-2 text-muted-foreground transition-colors hover:bg-surface-line hover:text-foreground" :disabled="isSavingHealthSettings" @click="closeHealthSettings">
            <X class="h-5 w-5" />
            <span class="sr-only">{{ t('admin.groupRates.actions.cancel') }}</span>
          </button>
        </div>

        <div v-if="isLoadingHealthSettings" class="flex min-h-[320px] items-center justify-center text-muted-foreground">
          <Loader2 class="mr-2 h-5 w-5 animate-spin" />
          {{ t('admin.groupRates.health.settings.loading') }}
        </div>

        <div v-else-if="healthSettingsDraft" class="min-h-0 flex-1 overflow-y-auto">
          <div class="p-6">
            <label v-if="healthSettingsGroupTypes.length" class="block w-full sm:w-72">
              <span class="mb-2 block text-sm font-medium text-foreground">{{ t('admin.groupRates.health.settings.groupTypeFilter') }}</span>
              <Select v-model="healthSettingsGroupType" class="w-full">
                <option v-for="groupType in healthSettingsGroupTypes" :key="groupType" :value="groupType">{{ groupType }}</option>
              </Select>
            </label>

            <section v-if="selectedHealthTypeDefaults" class="mt-6 border-t border-border/50 pt-6">
              <h3 class="mb-5 text-sm font-semibold text-foreground">
                {{ t('admin.groupRates.health.settings.typeDefaults', { type: healthSettingsGroupType }) }}
              </h3>
              <div class="flex items-center justify-between gap-6">
                <div class="font-medium text-foreground">{{ t('admin.groupRates.health.settings.enabled') }}</div>
                <button
                  type="button"
                  role="switch"
                  :aria-checked="selectedHealthTypeDefaults.enabled"
                  :class="[
                    'relative inline-flex h-6 w-11 shrink-0 items-center rounded-full border transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary',
                    selectedHealthTypeDefaults.enabled ? 'border-emerald-500/40 bg-emerald-500' : 'border-border/70 bg-muted',
                  ]"
                  @click="selectedHealthTypeDefaults.enabled = !selectedHealthTypeDefaults.enabled"
                >
                  <span :class="['h-4 w-4 rounded-full bg-white shadow-sm transition-transform', selectedHealthTypeDefaults.enabled ? 'translate-x-5' : 'translate-x-1']" />
                </button>
              </div>

              <div class="mt-6 grid gap-4 sm:grid-cols-3">
                <label class="block">
                  <span class="mb-2 block text-sm font-medium text-foreground">{{ t('admin.groupRates.health.settings.interval') }}</span>
                  <input v-model.number="selectedHealthTypeDefaults.probeIntervalSeconds" type="number" min="10" max="86400" step="1" class="h-11 w-full rounded-lg border border-border/70 bg-surface px-3 text-sm text-foreground outline-none focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30" />
                </label>
                <label class="block">
                  <span class="mb-2 block text-sm font-medium text-foreground">{{ t('admin.groupRates.health.settings.failureThreshold') }}</span>
                  <input v-model.number="selectedHealthTypeDefaults.failureThreshold" type="number" min="1" max="10" step="1" class="h-11 w-full rounded-lg border border-border/70 bg-surface px-3 text-sm text-foreground outline-none focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30" />
                </label>
                <label class="block">
                  <span class="mb-2 block text-sm font-medium text-foreground">{{ t('admin.groupRates.health.settings.defaultModel') }}</span>
                  <Input v-model="selectedHealthTypeDefaults.model" :placeholder="t('admin.groupRates.health.settings.modelPlaceholder')" autocomplete="off" />
                </label>
              </div>
            </section>

            <section v-if="selectedHealthTypeDefaults" class="mt-8 border-t border-border/50 pt-6">
              <h3 class="text-sm font-semibold text-foreground">{{ t('admin.groupRates.health.settings.groupOverrides') }}</h3>
              <div class="mt-4 overflow-x-auto rounded-lg border border-border/60">
                <table class="w-full min-w-[920px] text-sm">
                  <thead class="border-b border-border/60 bg-surface-elevated/70">
                    <tr>
                      <th class="px-4 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.health.settings.group') }}</th>
                      <th class="px-4 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.health.settings.autoProbe') }}</th>
                      <th class="px-4 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.health.settings.model') }}</th>
                      <th class="px-4 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.health.settings.groupInterval') }}</th>
                      <th class="px-4 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.health.settings.groupFailureThreshold') }}</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-border/50">
                    <tr v-for="group in filteredHealthSettingsGroups" :key="`${group.upstreamSiteId}-${group.upstreamGroupId || group.upstreamGroupName}`">
                      <td class="px-4 py-3">
                        <div class="font-medium text-foreground">{{ group.upstreamGroupName }}</div>
                        <div class="mt-0.5 text-xs text-muted-foreground">{{ group.upstreamSiteName || group.upstreamSiteId }}</div>
                      </td>
                      <td class="px-4 py-3 text-center">
                        <button
                          type="button"
                          role="switch"
                          :aria-checked="group.enabled"
                          :aria-label="t('admin.groupRates.health.settings.groupAutoProbeLabel', { group: group.upstreamGroupName })"
                          :class="[
                            'relative inline-flex h-6 w-11 shrink-0 items-center rounded-full border transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary',
                            group.enabled ? 'border-emerald-500/40 bg-emerald-500' : 'border-border/70 bg-muted',
                          ]"
                          @click="group.enabled = !group.enabled"
                        >
                          <span :class="['h-4 w-4 rounded-full bg-white shadow-sm transition-transform', group.enabled ? 'translate-x-5' : 'translate-x-1']" />
                        </button>
                      </td>
                      <td class="w-[260px] px-4 py-3">
                        <Input
                          v-model="group.model"
                          :placeholder="inheritedModelForGroup(group) || t('admin.groupRates.health.settings.modelPlaceholder')"
                          autocomplete="off"
                        />
                      </td>
                      <td class="w-[150px] px-4 py-3">
                        <input
                          :value="group.probeIntervalSeconds ?? ''"
                          type="number"
                          min="10"
                          max="86400"
                          step="1"
                          :placeholder="String(inheritedIntervalForGroup(group))"
                          class="h-10 w-full rounded-lg border border-border/70 bg-surface px-3 text-center text-sm text-foreground outline-none focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30"
                          @input="setGroupProbeInterval(group, $event)"
                        />
                      </td>
                      <td class="w-[150px] px-4 py-3">
                        <input
                          :value="group.failureThreshold ?? ''"
                          type="number"
                          min="1"
                          max="10"
                          step="1"
                          :placeholder="String(inheritedFailureThresholdForGroup(group))"
                          class="h-10 w-full rounded-lg border border-border/70 bg-surface px-3 text-center text-sm text-foreground outline-none focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30"
                          @input="setGroupFailureThreshold(group, $event)"
                        />
                      </td>
                    </tr>
                    <tr v-if="filteredHealthSettingsGroups.length === 0">
                      <td colspan="5" class="px-4 py-8 text-center text-muted-foreground">{{ t('admin.groupRates.health.settings.noGroups') }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>
          </div>
        </div>

        <div class="border-t border-border/50 p-6">
          <div v-if="healthSettingsErrorKey" class="mb-4 flex items-start gap-2 text-sm text-destructive">
            <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
            <span>{{ t(healthSettingsErrorKey) }}</span>
          </div>
          <div class="flex justify-end gap-3">
            <Button variant="secondary" :disabled="isSavingHealthSettings" @click="closeHealthSettings">{{ t('admin.groupRates.actions.cancel') }}</Button>
            <Button :disabled="isSavingHealthSettings || isLoadingHealthSettings || !healthSettingsDraft" @click="saveHealthSettings">
              <Loader2 v-if="isSavingHealthSettings" class="mr-2 h-4 w-4 animate-spin" />
              {{ t('admin.groupRates.health.settings.save') }}
            </Button>
          </div>
        </div>
      </div>
    </div>

    <div v-if="isHistoryOpen" class="fixed inset-0 z-50 flex items-center justify-center bg-background/80 p-4 backdrop-blur-sm">
      <div data-group-rates-dialog role="dialog" aria-modal="true" aria-labelledby="group-rate-history-title" tabindex="-1" class="max-h-[calc(100dvh-2rem)] w-full max-w-4xl overflow-hidden overscroll-contain rounded-xl border border-border/50 bg-card shadow-xl">
        <div class="flex items-start justify-between gap-4 border-b border-border/50 p-6">
          <div>
            <h2 id="group-rate-history-title" class="text-xl font-semibold text-foreground">{{ historyTitle }}</h2>
            <p v-if="selectedRate" class="mt-2 text-sm text-muted-foreground">
              {{ t('admin.groupRates.history.subtitle', { platform: platformLabel(selectedRate.platform) }) }}
            </p>
          </div>
          <button class="rounded-lg p-2 text-muted-foreground transition-colors hover:bg-surface-line hover:text-foreground" @click="closeHistory">
            <X class="h-5 w-5" />
            <span class="sr-only">{{ t('admin.groupRates.actions.closeHistory') }}</span>
          </button>
        </div>

        <div v-if="historyErrorKey" class="m-6 flex items-start gap-3 rounded-xl border border-warning/20 bg-warning/10 p-4 text-sm text-warning">
          <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ t(historyErrorKey) }}</span>
        </div>

        <div v-if="isHistoryLoading" class="flex min-h-[260px] items-center justify-center text-muted-foreground">
          <Loader2 class="mr-2 h-5 w-5 animate-spin" />
          {{ t('admin.groupRates.history.loading') }}
        </div>

        <div v-else-if="history.length === 0" class="flex min-h-[260px] flex-col items-center justify-center px-6 text-center">
          <History class="h-8 w-8 text-muted-foreground" />
          <h3 class="mt-4 font-semibold text-foreground">{{ t('admin.groupRates.history.emptyTitle') }}</h3>
          <p class="mt-2 text-sm text-muted-foreground">{{ t('admin.groupRates.history.emptyDescription') }}</p>
        </div>

        <div v-else class="max-h-[60vh] overflow-auto">
          <table class="w-full min-w-[680px] text-sm">
            <thead class="sticky top-0 border-b border-border/50 bg-surface-elevated">
              <tr>
                <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.siteName') }}</th>
                <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.groupName') }}</th>
                <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.type') }}</th>
                <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.fields.platform') }}</th>
                <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.history.multiplier') }}</th>
                <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.history.delta') }}</th>
                <th class="px-6 py-3 text-center font-medium text-muted-foreground">{{ t('admin.groupRates.history.createdAt') }}</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-border/50">
              <tr v-for="(row, index) in history" :key="historyRowKey(row, index)" class="transition-colors hover:bg-surface/30">
                <td class="px-6 py-4 font-medium text-foreground">{{ row.siteName }}</td>
                <td class="px-6 py-4 text-foreground">
                  <div class="flex items-center justify-center gap-1.5">
                    <span>{{ row.groupName }}</span>
                    <span v-if="row.deleted" class="inline-flex rounded-md border border-red-500/20 bg-red-500/10 px-1.5 py-0.5 text-[10px] font-semibold text-red-500">{{ t('admin.groupRates.status.deleted') }}</span>
                  </div>
                </td>
                <td class="px-6 py-4 text-muted-foreground">{{ typeLabel(row.type) }}</td>
                <td class="px-6 py-4 text-muted-foreground">{{ platformLabel(row.platform) }}</td>
                <td class="px-6 py-4">
                  <span class="font-semibold text-foreground">{{ formatMultiplier(row.currentMultiplier ?? row.multiplier) }}</span>
                  <span v-if="row.currentMultiplier !== null && row.currentMultiplier !== row.multiplier" class="ml-1 text-[10px] text-muted-foreground">{{ formatMultiplier(row.multiplier) }}</span>
                </td>
                <td class="px-6 py-4">
                  <span
                    class="inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-semibold"
                    :class="deltaClasses(row.delta)"
                  >
                    {{ formatDelta(row.delta) }}
                  </span>
                </td>
                <td class="px-6 py-4 text-muted-foreground">{{ formatDateTime(row.createdAt) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div v-if="editingRate" class="fixed inset-0 z-50 flex items-center justify-center bg-background/80 p-4 backdrop-blur-sm">
      <div data-group-rates-dialog role="dialog" aria-modal="true" aria-labelledby="group-rate-edit-title" tabindex="-1" class="max-h-[calc(100dvh-2rem)] w-full max-w-md overflow-y-auto overscroll-contain rounded-xl border border-border/50 bg-card shadow-xl">
        <div class="flex items-start justify-between gap-4 border-b border-border/50 p-6">
          <div>
            <h2 id="group-rate-edit-title" class="text-xl font-semibold text-foreground">{{ editTypeTitle }}</h2>
            <p class="mt-2 text-sm text-muted-foreground">{{ t('admin.groupRates.edit.description') }}</p>
          </div>
          <button class="rounded-lg p-2 text-muted-foreground transition-colors hover:bg-surface-line hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary" :aria-label="t('admin.groupRates.actions.cancel')" :disabled="isActionLoading" @click="closeTypeEditor">
            <X class="h-5 w-5" />
            <span class="sr-only">{{ t('admin.groupRates.actions.closeEdit') }}</span>
          </button>
        </div>

        <form class="space-y-5 p-6" @submit.prevent="submitTypeEditor">
          <label class="block space-y-2">
            <span class="text-sm font-medium text-foreground">{{ t('admin.groupRates.edit.typeLabel') }}</span>
            <Select v-model="editTypeValue" :disabled="isActionLoading">
              <option value="">{{ t('admin.groupRates.edit.typePlaceholder') }}</option>
              <option v-for="type in editTypeOptions" :key="type" :value="type">{{ typeLabel(type) }}</option>
            </Select>
          </label>

          <div class="flex justify-end gap-2">
            <Button type="button" variant="secondary" :disabled="isActionLoading" @click="closeTypeEditor">
              {{ t('admin.groupRates.actions.cancel') }}
            </Button>
            <Button type="submit" class="gap-2" :disabled="isActionLoading">
              <Loader2 v-if="isActionLoading" class="h-4 w-4 animate-spin" />
              {{ t('admin.groupRates.actions.saveType') }}
            </Button>
          </div>
        </form>
      </div>
    </div>

    <div v-if="connectingRate" class="fixed inset-0 z-50 flex items-center justify-center bg-background/80 p-4 backdrop-blur-sm">
      <div data-group-rates-dialog role="dialog" aria-modal="true" aria-labelledby="group-rate-connect-title" tabindex="-1" class="max-h-[calc(100dvh-2rem)] w-full max-w-2xl overflow-y-auto overscroll-contain rounded-lg border border-border/60 bg-card shadow-xl">
        <div class="flex items-start justify-between gap-4 border-b border-border/50 p-6">
          <div>
            <h2 id="group-rate-connect-title" class="text-xl font-semibold text-foreground">
              {{ t('admin.groupRates.connect.titleWithGroup', { site: connectingRate.siteName, group: connectingRate.groupName }) }}
            </h2>
            <p class="mt-2 text-sm text-muted-foreground">
              {{ connectMode === 'bind' ? t('admin.groupRates.connect.bindDescription') : t('admin.groupRates.connect.realDescription') }}
            </p>
          </div>
          <button class="rounded-lg p-2 text-muted-foreground transition-colors hover:bg-surface-line hover:text-foreground" :disabled="isActionLoading" @click="closeConnector">
            <X class="h-5 w-5" />
            <span class="sr-only">{{ t('admin.groupRates.actions.closeConnect') }}</span>
          </button>
        </div>

        <form class="space-y-5 p-6" @submit.prevent="submitConnector">
          <div class="grid gap-3 sm:grid-cols-2">
            <button
              type="button"
              :class="[
                'flex min-h-24 items-start gap-3 rounded-lg border p-4 text-left transition-colors',
                connectMode === 'real'
                  ? 'border-primary bg-primary/5 text-foreground'
                  : 'border-border/60 bg-surface text-muted-foreground hover:border-primary/40 hover:text-foreground'
              ]"
              @click="setConnectMode('real')"
            >
              <span class="flex h-9 w-9 shrink-0 items-center justify-center rounded-md bg-primary/10 text-primary">
                <Sparkles class="h-4 w-4" />
              </span>
              <span class="min-w-0">
                <span class="flex items-center gap-2 text-sm font-semibold">
                  {{ t('admin.groupRates.connect.modeReal') }}
                  <Check v-if="connectMode === 'real'" class="h-4 w-4 text-primary" />
                </span>
                <span class="mt-1 block text-xs leading-5 text-muted-foreground">{{ t('admin.groupRates.connect.realDescription') }}</span>
              </span>
            </button>
            <button
              type="button"
              :class="[
                'flex min-h-24 items-start gap-3 rounded-lg border p-4 text-left transition-colors',
                connectMode === 'bind'
                  ? 'border-primary bg-primary/5 text-foreground'
                  : 'border-border/60 bg-surface text-muted-foreground hover:border-primary/40 hover:text-foreground'
              ]"
              @click="setConnectMode('bind')"
            >
              <span class="flex h-9 w-9 shrink-0 items-center justify-center rounded-md bg-primary/10 text-primary">
                <Link2 class="h-4 w-4" />
              </span>
              <span class="min-w-0">
                <span class="flex items-center gap-2 text-sm font-semibold">
                  {{ t('admin.groupRates.connect.modeBind') }}
                  <Check v-if="connectMode === 'bind'" class="h-4 w-4 text-primary" />
                </span>
                <span class="mt-1 block text-xs leading-5 text-muted-foreground">{{ t('admin.groupRates.connect.bindDescription') }}</span>
              </span>
            </button>
          </div>

          <div class="rounded-xl border border-border/50 bg-surface/50 p-4 space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-medium text-muted-foreground">{{ t('admin.groupRates.connect.upstreamSiteLabel') }}</span>
              <span class="text-sm font-medium text-foreground">{{ connectingRate?.siteName }}</span>
            </div>
            <div class="flex items-center justify-between">
              <span class="text-xs font-medium text-muted-foreground">{{ t('admin.groupRates.connect.upstreamGroupNameLabel') }}</span>
              <span class="text-sm font-medium text-foreground">{{ connectingRate?.groupName }}</span>
            </div>
            <div class="flex items-center justify-between">
              <span class="text-xs font-medium text-muted-foreground">{{ t('admin.groupRates.connect.upstreamMultiplierLabel') }}</span>
              <span class="text-sm font-semibold text-primary">{{ formatMultiplier(connectingRate?.currentMultiplier ?? null) }}</span>
            </div>
            <div class="flex items-center justify-between">
              <span class="text-xs font-medium text-muted-foreground">{{ t('admin.groupRates.connect.upstreamPlatformLabel') }}</span>
              <span :class="['inline-flex rounded-md border px-2 py-0.5 text-xs font-semibold uppercase tracking-wider', platformClasses(connectingRate?.platform ?? null)]">
                {{ platformLabel(connectingRate?.platform ?? null) }}
              </span>
            </div>
          </div>

          <!-- sub2api admin：分组类型选择（仅在无法自动检测时显示） -->
          <div v-if="needsGroupTypeSelection" class="space-y-2">
            <span class="text-sm font-medium text-foreground">{{ t('admin.groupRates.connect.groupTypeLabel') }}</span>
            <Select v-model="selectedGroupType" :disabled="isActionLoading">
              <option value="">{{ t('admin.groupRates.connect.groupTypePlaceholder') }}</option>
              <option value="openai">{{ t('admin.groupRates.connect.groupTypeOpenai') }}</option>
              <option value="anthropic">{{ t('admin.groupRates.connect.groupTypeAnthropic') }}</option>
              <option value="gemini">{{ t('admin.groupRates.connect.groupTypeGemini') }}</option>
              <option value="antigravity">{{ t('admin.groupRates.connect.groupTypeAntigravity') }}</option>
            </Select>
          </div>

          <!-- new-api admin：渠道类型选择 -->
          <div v-if="needsChannelTypeSelection" class="space-y-2">
            <span class="text-sm font-medium text-foreground">{{ t('admin.groupRates.connect.channelTypeLabel') }}</span>
            <Select v-model.number="selectedChannelType" :disabled="isActionLoading">
              <option :value="0">{{ t('admin.groupRates.connect.channelTypePlaceholder') }}</option>
              <option v-for="ct in filteredChannelTypes" :key="ct.id" :value="ct.id">{{ ct.name }}</option>
            </Select>
          </div>

          <div v-if="connectMode === 'bind'" class="space-y-2">
            <span class="text-sm font-medium text-foreground">{{ t('admin.groupRates.connect.bindSelectKey') }}</span>
            <div v-if="isLoadingKeys" class="flex items-center justify-center py-6 text-muted-foreground">
              <Loader2 class="mr-2 h-4 w-4 animate-spin" />
              {{ t('admin.groupRates.connect.bindKeysLoading') }}
            </div>
            <div v-else-if="upstreamKeys.length === 0" class="px-4 py-6 text-center text-sm text-muted-foreground">
              {{ t('admin.groupRates.connect.bindKeysEmpty') }}
            </div>
            <div v-else class="max-h-48 overflow-auto rounded-xl border border-border/50 bg-surface divide-y divide-border/30">
              <label
                v-for="keyItem in upstreamKeys"
                :key="keyItem.id"
                :class="[
                  'flex items-center gap-3 px-4 py-2.5 cursor-pointer transition-colors',
                  selectedKeyId === keyItem.id ? 'bg-primary/5' : 'hover:bg-surface-elevated'
                ]"
              >
                <input
                  type="radio"
                  :value="keyItem.id"
                  :checked="selectedKeyId === keyItem.id"
                  class="h-4 w-4 border-border text-primary focus:ring-primary"
                  :disabled="isActionLoading"
                  @change="selectedKeyId = keyItem.id"
                />
                <div class="flex-1 min-w-0">
                  <div class="text-sm font-medium text-foreground truncate">{{ keyItem.name }}</div>
                  <div v-if="keyItem.keyPreview" class="text-xs text-muted-foreground font-mono truncate">{{ keyItem.keyPreview }}</div>
                </div>
                <span v-if="keyItem.groupName" class="inline-flex rounded-md border border-border/50 bg-surface-elevated px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground shrink-0">
                  {{ keyItem.groupName }}
                </span>
                <span
                  :class="[
                    'inline-flex rounded-md border px-1.5 py-0.5 text-[10px] font-semibold shrink-0',
                    isActiveResourceStatus(keyItem.status)
                      ? 'border-emerald-400/30 bg-emerald-500/10 text-emerald-600 dark:text-emerald-300'
                      : 'border-border/60 bg-surface-elevated text-muted-foreground'
                  ]"
                >
                  {{ resourceStatusLabel(keyItem.status) }}
                </span>
              </label>
            </div>
          </div>

          <div v-if="connectMode === 'bind'" class="space-y-2">
            <label for="existing-admin-group" class="flex items-center gap-2 text-sm font-medium text-foreground">
              <ServerCog class="h-4 w-4 text-primary" />
              {{ t('admin.groupRates.connect.bindSelectAdminGroup') }}
            </label>
            <Select
              id="existing-admin-group"
              :value="selectedAdminGroupId"
              :disabled="isActionLoading || isLoadingAdminResources"
              @change="handleAdminGroupChange"
            >
              <option value="">{{ t('admin.groupRates.connect.bindAdminGroupPlaceholder') }}</option>
              <option v-for="group in ownGroups" :key="group.id" :value="group.id">
                {{ group.groupName }} · {{ formatMultiplier(group.multiplier) }}
              </option>
            </Select>
          </div>

          <div v-if="connectMode === 'bind' && selectedAdminGroupId" class="space-y-2">
            <span class="flex items-center gap-2 text-sm font-medium text-foreground">
              <ServerCog class="h-4 w-4 text-primary" />
              {{ t('admin.groupRates.connect.bindSelectAdminResource') }}
            </span>
            <div v-if="isLoadingAdminResources" class="flex items-center justify-center py-6 text-muted-foreground">
              <Loader2 class="mr-2 h-4 w-4 animate-spin" />
              {{ t('admin.groupRates.connect.adminResourcesLoading') }}
            </div>
            <div v-else-if="adminResources.length === 0" class="rounded-lg border border-dashed border-border/70 px-4 py-6 text-center text-sm text-muted-foreground">
              {{ t('admin.groupRates.connect.adminResourcesEmpty') }}
            </div>
            <div v-else class="max-h-48 divide-y divide-border/30 overflow-auto rounded-lg border border-border/60 bg-surface">
              <label
                v-for="resource in adminResources"
                :key="resource.id"
                class="flex cursor-pointer items-center gap-3 px-4 py-3 transition-colors hover:bg-surface-elevated"
                :class="selectedAdminResourceId === resource.id ? 'bg-primary/5' : ''"
              >
                <input
                  v-model="selectedAdminResourceId"
                  type="radio"
                  :value="resource.id"
                  class="h-4 w-4 border-border text-primary focus:ring-primary"
                  :disabled="isActionLoading"
                />
                <div class="min-w-0 flex-1">
                  <div class="truncate text-sm font-medium text-foreground">{{ resource.name }}</div>
                  <div class="mt-0.5 truncate text-xs text-muted-foreground">{{ adminResourceTypeLabel(resource) }}</div>
                </div>
                <span :class="['rounded-md border px-2 py-0.5 text-[10px] font-semibold', isActiveResourceStatus(resource.status) ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-600 dark:text-emerald-300' : 'border-border/60 bg-surface-elevated text-muted-foreground']">
                  {{ resourceStatusLabel(resource.status) }}
                </span>
              </label>
            </div>
          </div>

          <div v-if="connectMode === 'real'" class="space-y-2">
            <span class="text-sm font-medium text-foreground">{{ t('admin.groupRates.connect.ownGroupLabel') }}</span>
            <div class="max-h-48 overflow-auto rounded-xl border border-border/50 bg-surface divide-y divide-border/30">
              <label
                v-for="group in filteredOwnGroups"
                :key="group.id"
                class="flex items-center gap-3 px-4 py-2.5 cursor-pointer transition-colors hover:bg-surface-elevated"
              >
                <input
                  type="checkbox"
                  :checked="connectOwnGroups.includes(group.id)"
                  class="h-4 w-4 rounded border-border text-primary focus:ring-primary"
                  :disabled="isActionLoading"
                  @change="toggleOwnGroup(group.id)"
                />
                <span class="text-sm text-foreground">{{ group.groupName }}</span>
                <span v-if="group.platform" class="inline-flex rounded-md border border-border/50 bg-surface-elevated px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                  {{ group.platform }}
                </span>
                <span class="ml-auto text-xs text-muted-foreground">{{ formatMultiplier(group.multiplier) }}</span>
              </label>
              <div v-if="filteredOwnGroups.length === 0" class="px-4 py-3 text-sm text-muted-foreground">
                {{ t('admin.groupRates.connect.ownGroupPlaceholder') }}
              </div>
            </div>
          </div>

          <label class="flex cursor-pointer items-start gap-3 rounded-lg border border-border/60 bg-surface p-4 transition-colors hover:border-primary/40">
            <input
              v-model="addToPricingMapping"
              type="checkbox"
              class="mt-0.5 h-4 w-4 rounded border-border text-primary focus:ring-primary"
              :disabled="isActionLoading"
            />
            <span>
              <span class="flex items-center gap-2 text-sm font-medium text-foreground">
                <KeyRound class="h-4 w-4 text-primary" />
                {{ t('admin.groupRates.connect.addToPricingMapping') }}
              </span>
              <span class="mt-1 block text-xs leading-5 text-muted-foreground">{{ t('admin.groupRates.connect.addToPricingMappingHint') }}</span>
            </span>
          </label>

          <div v-if="realConnectError" class="flex items-start gap-3 rounded-xl border border-warning/20 bg-warning/10 p-3 text-sm text-warning">
            <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
            <span>{{ realConnectError }}</span>
          </div>

          <div class="flex justify-end gap-2">
            <Button type="button" variant="secondary" :disabled="isActionLoading" @click="closeConnector">
              {{ t('admin.groupRates.actions.cancel') }}
            </Button>
            <Button type="submit" class="gap-2" :disabled="isActionLoading || !canSubmitConnect">
              <Loader2 v-if="isActionLoading" class="h-4 w-4 animate-spin" />
              {{ t(connectMode === 'real' ? 'admin.groupRates.connect.submitManaged' : 'admin.groupRates.connect.submitExisting') }}
            </Button>
          </div>
        </form>
      </div>
    </div>

    <div v-if="connectionEditingRate" class="fixed inset-0 z-50 flex items-center justify-center bg-background/80 p-4 backdrop-blur-sm">
      <div data-group-rates-dialog role="dialog" aria-modal="true" aria-labelledby="group-rate-connection-edit-title" tabindex="-1" class="max-h-[calc(100dvh-2rem)] w-full max-w-2xl overflow-y-auto overscroll-contain rounded-lg border border-border/60 bg-card shadow-xl">
        <div class="flex items-start justify-between gap-4 border-b border-border/50 p-6">
          <div>
            <h2 id="group-rate-connection-edit-title" class="text-xl font-semibold text-foreground">
              {{ t('admin.groupRates.connectionEdit.title', { site: connectionEditingRate.siteName, group: connectionEditingRate.groupName }) }}
            </h2>
            <p class="mt-2 text-sm text-muted-foreground">{{ t('admin.groupRates.connectionEdit.description') }}</p>
          </div>
          <button type="button" class="rounded-lg p-2 text-muted-foreground transition-colors hover:bg-surface-line hover:text-foreground" :disabled="isUpdatingConnectionGroups" :aria-label="t('admin.groupRates.actions.cancel')" @click="closeConnectionEditor">
            <X class="h-5 w-5" />
          </button>
        </div>

        <form class="space-y-5 p-6" @submit.prevent="submitConnectionEditor">
          <div class="rounded-lg border border-border/60 bg-surface px-4 py-3">
            <div class="flex items-center justify-between gap-4 text-sm">
              <span class="text-muted-foreground">{{ t('admin.groupRates.fields.upstreamGroup') }}</span>
              <span class="font-medium text-foreground">{{ connectionEditingRate.groupName }} {{ formatMultiplier(connectionEditingRate.currentMultiplier) }}</span>
            </div>
          </div>

          <div class="space-y-2">
            <span class="text-sm font-medium text-foreground">{{ t('admin.groupRates.connectionEdit.ownGroupLabel') }}</span>
            <div class="max-h-72 divide-y divide-border/30 overflow-auto rounded-lg border border-border/60 bg-surface">
              <label v-for="group in ownGroups" :key="group.id" class="flex cursor-pointer items-center gap-3 px-4 py-3 transition-colors hover:bg-surface-elevated">
                <input
                  type="checkbox"
                  :checked="editConnectionGroups.includes(group.id)"
                  class="h-4 w-4 rounded border-border text-primary focus:ring-primary"
                  :disabled="isUpdatingConnectionGroups"
                  @change="toggleEditConnectionGroup(group.id)"
                />
                <span class="min-w-0 flex-1 truncate text-sm font-medium text-foreground">{{ group.groupName }}</span>
                <span v-if="group.platform" class="inline-flex rounded-md border border-border/50 bg-surface-elevated px-1.5 py-0.5 text-[10px] font-semibold uppercase text-muted-foreground">{{ group.platform }}</span>
                <span class="shrink-0 text-xs tabular-nums text-muted-foreground">{{ formatMultiplier(group.multiplier) }}</span>
              </label>
              <div v-if="ownGroups.length === 0" class="px-4 py-8 text-center text-sm text-muted-foreground">{{ t('admin.groupRates.connectionEdit.empty') }}</div>
            </div>
          </div>

          <div v-if="editConnectionError" class="flex items-start gap-3 rounded-lg border border-warning/20 bg-warning/10 p-3 text-sm text-warning">
            <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
            <span>{{ editConnectionError }}</span>
          </div>

          <div class="flex justify-end gap-2">
            <Button type="button" variant="secondary" :disabled="isUpdatingConnectionGroups" @click="closeConnectionEditor">
              {{ t('admin.groupRates.actions.cancel') }}
            </Button>
            <Button type="submit" class="gap-2" :disabled="isUpdatingConnectionGroups">
              <Loader2 v-if="isUpdatingConnectionGroups" class="h-4 w-4 animate-spin" />
              {{ t('admin.groupRates.actions.confirm') }}
            </Button>
          </div>
        </form>
      </div>
    </div>

    <div v-if="disconnectingRate" class="fixed inset-0 z-50 flex items-center justify-center bg-background/80 p-4 backdrop-blur-sm">
      <div data-group-rates-dialog role="dialog" aria-modal="true" aria-labelledby="group-rate-disconnect-title" tabindex="-1" class="max-h-[calc(100dvh-2rem)] w-full max-w-md overflow-y-auto overscroll-contain rounded-xl border border-border/50 bg-card shadow-xl">
        <div class="flex items-start justify-between gap-4 border-b border-border/50 p-6">
          <div>
            <h2 id="group-rate-disconnect-title" class="text-xl font-semibold text-foreground">{{ t('admin.groupRates.disconnect.title') }}</h2>
            <p class="mt-2 text-sm text-muted-foreground">
              {{ t('admin.groupRates.disconnect.description', { site: disconnectingRate.siteName, group: disconnectingRate.groupName }) }}
            </p>
          </div>
          <button class="rounded-lg p-2 text-muted-foreground transition-colors hover:bg-surface-line hover:text-foreground" :disabled="isDisconnecting" @click="closeDisconnect">
            <X class="h-5 w-5" />
          </button>
        </div>

        <div v-if="disconnectError" class="mx-6 mt-6 flex items-start gap-3 rounded-xl border border-warning/20 bg-warning/10 p-3 text-sm text-warning">
          <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ disconnectError }}</span>
        </div>

        <div class="space-y-4 p-6">
          <div class="space-y-2">
            <div class="flex items-center justify-between gap-3">
              <div>
                <p class="text-sm font-medium text-foreground">{{ t('admin.groupRates.disconnect.downstreamGroups') }}</p>
                <p class="mt-1 text-xs text-muted-foreground">{{ t('admin.groupRates.disconnect.downstreamGroupsHint') }}</p>
              </div>
              <span class="shrink-0 text-xs text-muted-foreground">{{ disconnectTargetCount }}/{{ disconnectTargets.length }}</span>
            </div>
            <div v-if="disconnectTargets.length" class="max-h-48 space-y-2 overflow-y-auto rounded-lg border border-border/60 bg-surface p-3">
              <label
                v-for="target in disconnectTargets"
                :key="target.key"
                class="flex cursor-pointer items-start gap-3 rounded-md px-2 py-2 transition-colors hover:bg-surface-elevated"
              >
                <input
                  v-model="disconnectSelectedTargets"
                  type="checkbox"
                  :value="target.key"
                  class="mt-0.5 h-4 w-4 rounded border-border text-primary focus:ring-primary"
                  :disabled="isDisconnecting"
                />
                <span class="min-w-0 text-sm text-foreground">
                  <span class="block truncate">{{ target.name }}</span>
                  <span v-if="target.accountName" class="mt-0.5 block truncate text-xs text-muted-foreground">{{ target.accountName }}</span>
                </span>
              </label>
            </div>
            <p v-else class="rounded-lg border border-warning/20 bg-warning/10 p-3 text-xs text-warning">{{ t('admin.groupRates.disconnect.noTargets') }}</p>
          </div>

          <div class="space-y-3">
            <label
              class="flex cursor-pointer items-start gap-3 rounded-xl border p-4 transition-colors"
              :class="disconnectMode === 'unlink'
                ? 'border-primary bg-primary/5'
                : 'border-border/50 bg-surface hover:bg-surface-elevated'"
            >
              <input
                v-model="disconnectMode"
                type="radio"
                value="unlink"
                class="mt-0.5 h-4 w-4 border-border text-primary focus:ring-primary"
                :disabled="isDisconnecting"
              />
              <div>
                <span class="text-sm font-medium text-foreground">{{ t('admin.groupRates.disconnect.unlinkOnly') }}</span>
                <p class="mt-1 text-xs text-muted-foreground">{{ t('admin.groupRates.disconnect.unlinkOnlyHint') }}</p>
              </div>
            </label>

            <label
              v-if="disconnectCanDeleteRemote"
              class="flex cursor-pointer items-start gap-3 rounded-xl border p-4 transition-colors"
              :class="disconnectMode === 'full'
                ? 'border-red-500/50 bg-red-500/5'
                : 'border-border/50 bg-surface hover:bg-surface-elevated'"
            >
              <input
                v-model="disconnectMode"
                type="radio"
                value="full"
                class="mt-0.5 h-4 w-4 border-border text-red-500 focus:ring-red-500"
                :disabled="isDisconnecting"
              />
              <div>
                <span class="text-sm font-medium text-red-600 dark:text-red-400">{{ t('admin.groupRates.disconnect.deleteAll') }}</span>
                <p class="mt-1 text-xs text-red-500/70">{{ t('admin.groupRates.disconnect.deleteAllHint') }}</p>
              </div>
            </label>
          </div>

          <label
            v-if="disconnectConnection?.pricingMappingEnabled || disconnectingRate?.pricingMapped"
            class="flex cursor-pointer items-start gap-3 rounded-lg border border-border/60 bg-surface p-4"
          >
            <input
              v-model="disconnectRemovePricing"
              type="checkbox"
              class="mt-0.5 h-4 w-4 rounded border-border text-primary focus:ring-primary"
              :disabled="isDisconnecting"
            />
            <span>
              <span class="text-sm font-medium text-foreground">{{ t('admin.groupRates.disconnect.removePricingMapping') }}</span>
              <span class="mt-1 block text-xs text-muted-foreground">{{ t('admin.groupRates.disconnect.removePricingMappingHint') }}</span>
            </span>
          </label>

          <div class="flex justify-end gap-2">
            <Button variant="secondary" :disabled="isDisconnecting" @click="closeDisconnect">
              {{ t('admin.groupRates.actions.cancel') }}
            </Button>
            <Button
              :variant="disconnectMode === 'full' ? 'destructive' : 'default'"
              class="gap-2"
              :disabled="isDisconnecting || disconnectTargetCount === 0"
              @click="submitDisconnect"
            >
              <Loader2 v-if="isDisconnecting" class="h-4 w-4 animate-spin" />
              {{ t('admin.groupRates.disconnect.confirm') }}
            </Button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
