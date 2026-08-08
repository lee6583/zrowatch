<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { Search, Plus, CheckCircle2, XCircle, X, Loader2, AlertCircle, Trash2, Edit2, RefreshCw, Settings2, PowerOff, CircleHelp, ArrowUpDown } from 'lucide-vue-next'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { Tooltip } from '@/components/ui/tooltip'
import { getDownstreamConsumption, getMySiteMappingOptions, listRealConnections } from '../api/mySites'
import { getStrategySettings } from '../api/settings'
import { useUpstreamSites } from '../composables/useUpstreamSites'
import SiteSettingsModal from '../components/upstream/SiteSettingsModal.vue'
import type { UpstreamGroupInfo, UpstreamMetricValue, UpstreamSite, UpstreamSiteForm, UpstreamStatus } from '../types/upstream'
import type { DownstreamConsumptionItem, DownstreamConsumptionStatus } from '../types/mySites'

const { t, te, locale } = useI18n()

const upstreamFilterStorageKey = 'transit-hub:admin:upstream-filters.v1'

type StoredUpstreamFilters = {
  searchQuery?: string
  platformFilter?: string
  connectedGroupTypeFilter?: string
  sortMode?: string
}

type UpstreamSortMode = 'default' | 'balanceDesc' | 'balanceAsc' | 'historyRechargeDesc' | 'historyRechargeAsc'

const upstreamSortModes = new Set<UpstreamSortMode>([
  'default',
  'balanceDesc',
  'balanceAsc',
  'historyRechargeDesc',
  'historyRechargeAsc',
])

const isUpstreamSortMode = (value: unknown): value is UpstreamSortMode => (
  typeof value === 'string' && upstreamSortModes.has(value as UpstreamSortMode)
)

const normalizeSiteSearchValue = (value: string): string => (
  value
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .replace(/\/+$/, '')
)

const readStoredUpstreamFilters = (): StoredUpstreamFilters => {
  try {
    const raw = window.localStorage.getItem(upstreamFilterStorageKey)
    if (!raw) return {}
    const parsed = JSON.parse(raw) as StoredUpstreamFilters
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

const writeStoredUpstreamFilters = (filters: StoredUpstreamFilters): void => {
  try {
    window.localStorage.setItem(upstreamFilterStorageKey, JSON.stringify(filters))
  } catch {
    // Storage may be unavailable in private browsing or restricted environments.
  }
}

const storedFilters = readStoredUpstreamFilters()
const searchQuery = ref(typeof storedFilters.searchQuery === 'string' ? storedFilters.searchQuery : '')
const platformFilter = ref(typeof storedFilters.platformFilter === 'string' ? storedFilters.platformFilter : '')
const connectedGroupTypeFilter = ref(typeof storedFilters.connectedGroupTypeFilter === 'string' ? storedFilters.connectedGroupTypeFilter : '')
const sortMode = ref<UpstreamSortMode>(isUpstreamSortMode(storedFilters.sortMode) ? storedFilters.sortMode : 'default')
const connectedGroupKeys = ref(new Set<string>())
const downstreamConsumptionBySite = ref(new Map<string, DownstreamConsumptionItem>())
const isLoadingDownstreamConsumption = ref(false)
const downstreamConsumptionError = ref<string | null>(null)
const groupTargetKey = (siteId: string, groupValue: string): string => `${siteId}\u0000${groupValue.trim()}`
const addConnectedGroupKeys = (keys: Set<string>, siteId: string, groupId?: string, groupName?: string): void => {
  const normalizedSiteId = siteId.trim()
  if (!normalizedSiteId) return
  if (groupId?.trim()) keys.add(groupTargetKey(normalizedSiteId, groupId))
  if (groupName?.trim()) keys.add(groupTargetKey(normalizedSiteId, groupName))
}
const isAddModalOpen = ref(false)
const { sites: upstreamSites, isAdding, isRefreshing, addErrorKey, connectedCount, siteSyncStates, syncingSiteIds, addSite, updateSite, deleteSite, streamRefreshSites, refreshSingleSite } = useUpstreamSites()
const deletingSiteId = ref<string | null>(null)
const deleteErrorKey = ref<string | null>(null)
const editingSiteId = ref<string | null>(null)
const refreshIntervalSeconds = ref<number | null>(null)
const remainingSeconds = ref(0)
let countdownTimer: ReturnType<typeof window.setInterval> | null = null
const nextRefreshAtStorageKey = 'transit-hub:upstream-next-refresh-at'

const countdownDisplay = computed(() => {
  if (!refreshIntervalSeconds.value) return t('admin.upstream.refresh.disabled')
  return t('admin.upstream.refresh.countdown', { seconds: remainingSeconds.value })
})

const readNextRefreshAt = (): number | null => {
  const value = Number.parseInt(window.localStorage.getItem(nextRefreshAtStorageKey) ?? '', 10)
  if (!Number.isFinite(value) || value <= Date.now()) return null
  return value
}

const writeNextRefreshAt = (timestamp: number) => {
  window.localStorage.setItem(nextRefreshAtStorageKey, String(timestamp))
}

const updateRemainingSeconds = () => {
  const nextRefreshAt = readNextRefreshAt()
  remainingSeconds.value = nextRefreshAt ? Math.max(Math.ceil((nextRefreshAt - Date.now()) / 1000), 0) : 0
}

const scheduleNextRefresh = () => {
  if (!refreshIntervalSeconds.value) return
  writeNextRefreshAt(Date.now() + refreshIntervalSeconds.value * 1000)
  updateRemainingSeconds()
}

const runRefresh = async () => {
  if (isRefreshing.value) return
  await streamRefreshSites()
  await loadDownstreamConsumption()
  scheduleNextRefresh()
}

const startCountdown = (seconds: number) => {
  refreshIntervalSeconds.value = seconds
  const nextRefreshAt = readNextRefreshAt()
  if (!nextRefreshAt || nextRefreshAt > Date.now() + seconds * 1000) scheduleNextRefresh()
  updateRemainingSeconds()
  countdownTimer = window.setInterval(() => {
    if (!refreshIntervalSeconds.value || isRefreshing.value) return
    updateRemainingSeconds()
    if (remainingSeconds.value <= 0) void runRefresh()
  }, 1000)
}

const stopCountdown = () => {
  if (countdownTimer) window.clearInterval(countdownTimer)
  countdownTimer = null
}

const loadRefreshSettings = async () => {
  try {
    const settings = await getStrategySettings()
    if (!settings.enableRefreshInterval) return
    startCountdown(Math.max(settings.refreshInterval, 60))
  } catch (error) {
    refreshIntervalSeconds.value = null
  }
}

const createEmptyForm = (): UpstreamSiteForm => ({
  name: '',
  siteUrl: '',
  platform: 'auto',
  authMode: 'password',
  account: '',
  password: '',
  accessToken: '',
  refreshToken: '',
  tokenType: 'Bearer',
  userId: '',
  rechargeRate: 1,
  remark: '',
})

const newSiteForm = ref<UpstreamSiteForm>(createEmptyForm())

watch([searchQuery, platformFilter, connectedGroupTypeFilter, sortMode], () => {
  writeStoredUpstreamFilters({
    searchQuery: searchQuery.value,
    platformFilter: platformFilter.value,
    connectedGroupTypeFilter: connectedGroupTypeFilter.value,
    sortMode: sortMode.value,
  })
})

watch(
  () => newSiteForm.value.platform,
  (platform) => {
    if (platform === 'newapi' && newSiteForm.value.authMode === 'token') {
      newSiteForm.value.authMode = 'password'
    } else if (platform !== 'newapi' && newSiteForm.value.authMode === 'user_key') {
      newSiteForm.value.authMode = 'password'
    }
  },
)

const handleAddSite = async () => {
  const success = editingSiteId.value
    ? await updateSite(editingSiteId.value, newSiteForm.value)
    : await addSite(newSiteForm.value)
  if (!success) return
  isAddModalOpen.value = false
  newSiteForm.value = createEmptyForm()
  editingSiteId.value = null
}

const handleEditSite = (site: UpstreamSite) => {
  editingSiteId.value = site.id
  newSiteForm.value = {
    name: site.name,
    siteUrl: site.baseUrl,
    platform: site.platform,
    authMode: 'password',
    account: site.account,
    password: '',
    accessToken: '',
    refreshToken: '',
    tokenType: 'Bearer',
    userId: '',
    rechargeRate: site.rechargeRate > 0 ? site.rechargeRate : 1,
    remark: site.remark,
  }
  isAddModalOpen.value = true
}

const closeSiteModal = () => {
  isAddModalOpen.value = false
  editingSiteId.value = null
  newSiteForm.value = createEmptyForm()
}

const requestDeleteSite = (id: string) => {
  deletingSiteId.value = id
  deleteErrorKey.value = null
}

const cancelDeleteSite = () => {
  deletingSiteId.value = null
  deleteErrorKey.value = null
}

const confirmDeleteSite = async () => {
  if (!deletingSiteId.value) return
  try {
    await deleteSite(deletingSiteId.value)
    cancelDeleteSite()
  } catch (error) {
    deleteErrorKey.value = error instanceof Error ? error.message : 'admin.upstream.errors.unknown'
  }
}

const connectedGroupTypes = computed(() => {
  const types = new Set<string>()
  for (const site of upstreamSites.value) {
    for (const group of site.metrics.groups) {
      const type = group.platform?.trim()
      if (type && isGroupConnectedForSite(site.id, group)) types.add(type)
    }
  }
  return Array.from(types).sort((first, second) => first.localeCompare(second))
})

const hasLoadedConnectedGroupKeys = ref(false)

watch(connectedGroupTypes, (types) => {
  if (!hasLoadedConnectedGroupKeys.value) return
  if (connectedGroupTypeFilter.value && !types.some(type => type.toLowerCase() === connectedGroupTypeFilter.value.toLowerCase())) {
    connectedGroupTypeFilter.value = ''
  }
})

const filteredSites = computed(() => {
  const keyword = searchQuery.value.trim().toLowerCase()
  const normalizedKeyword = normalizeSiteSearchValue(searchQuery.value)
  const platform = platformFilter.value.toLowerCase()
  const groupType = connectedGroupTypeFilter.value.toLowerCase()

  const sites = upstreamSites.value.filter(site => {
    const baseUrl = site.baseUrl.toLowerCase()
    const matchesSearch = !keyword
      || site.name.toLowerCase().includes(keyword)
      || baseUrl.includes(keyword)
      || normalizeSiteSearchValue(baseUrl).includes(normalizedKeyword)
    const matchesPlatform = !platform || site.platform.toLowerCase() === platform
    const matchesGroupType = !groupType || connectedGroupsForSite(site).some(group => (
      group.platform?.toLowerCase() === groupType
    ))
    return matchesSearch && matchesPlatform && matchesGroupType
  })

  if (sortMode.value === 'default') return sites

  const metric = sortMode.value.startsWith('balance') ? 'balance' : 'historyRecharge'
  const direction = sortMode.value.endsWith('Asc') ? 1 : -1
  const sortValue = (site: UpstreamSite): number | null => {
    const value = site.metrics[metric].value
    if (value === null || !Number.isFinite(value) || !Number.isFinite(site.rechargeRate) || site.rechargeRate <= 0) return null
    return value * site.rechargeRate
  }

  return [...sites].sort((first, second) => {
    const firstValue = sortValue(first)
    const secondValue = sortValue(second)
    if (firstValue === null && secondValue === null) return first.name.localeCompare(second.name, locale.value)
    if (firstValue === null) return 1
    if (secondValue === null) return -1
    const difference = (firstValue - secondValue) * direction
    return difference || first.name.localeCompare(second.name, locale.value)
  })
})

const statusClasses: Record<UpstreamStatus, string> = {
  connecting: 'bg-primary/10 text-primary border-primary/20',
  syncing: 'bg-warning/10 text-warning border-warning/20',
  connected: 'bg-signal/10 text-signal border-signal/20',
  disabled: 'bg-destructive/10 text-destructive border-destructive/20',
  error: 'bg-warning/10 text-warning border-warning/20',
}

const statusLabel = (status: UpstreamStatus): string => t(`admin.upstream.status.${status}`)

const deletingSite = computed(() => upstreamSites.value.find((site) => site.id === deletingSiteId.value) ?? null)

// Groups Modal Logic
const isGroupsModalOpen = ref(false)
const selectedSiteForGroups = ref<UpstreamSite | null>(null)
const isLoadingGroupConnections = ref(false)

watch(upstreamSites, (sites) => {
  const selectedSiteId = selectedSiteForGroups.value?.id
  if (!selectedSiteId) return
  const latestSite = sites.find(site => site.id === selectedSiteId)
  if (latestSite && latestSite !== selectedSiteForGroups.value) selectedSiteForGroups.value = latestSite
})

const loadConnectedGroupKeys = async () => {
  isLoadingGroupConnections.value = true
  try {
    const [mappingOptions, realConnections] = await Promise.all([
      getMySiteMappingOptions().catch(() => ({ mappings: [] })),
      listRealConnections().catch(() => []),
    ])
    const nextKeys = new Set<string>()

    for (const mapping of mappingOptions.mappings ?? []) {
      for (const target of mapping.upstreamTargets ?? []) {
        addConnectedGroupKeys(nextKeys, target.siteId, undefined, target.groupName)
      }
    }
    for (const connection of realConnections) {
      if (connection.status && connection.status !== 'active') continue
      addConnectedGroupKeys(nextKeys, connection.upstreamSiteId, connection.upstreamGroupId, connection.upstreamGroupName)
    }

    connectedGroupKeys.value = nextKeys
  } finally {
    isLoadingGroupConnections.value = false
    hasLoadedConnectedGroupKeys.value = true
  }
}

const loadDownstreamConsumption = async () => {
  if (isLoadingDownstreamConsumption.value) return
  isLoadingDownstreamConsumption.value = true
  downstreamConsumptionError.value = null
  try {
    const response = await getDownstreamConsumption()
    downstreamConsumptionBySite.value = new Map(response.items.map(item => [item.siteId, item]))
  } catch (error) {
    downstreamConsumptionBySite.value = new Map()
    downstreamConsumptionError.value = error instanceof Error ? error.message : 'admin.upstream.errors.request'
  } finally {
    isLoadingDownstreamConsumption.value = false
  }
}

const refreshSiteWithUsage = async (siteId: string) => {
  await refreshSingleSite(siteId)
  await loadDownstreamConsumption()
}

const openGroupsModal = (site: UpstreamSite) => {
  selectedSiteForGroups.value = site
  isGroupsModalOpen.value = true
  void loadConnectedGroupKeys()
}

const closeGroupsModal = () => {
  isGroupsModalOpen.value = false
  selectedSiteForGroups.value = null
  isLoadingGroupConnections.value = false
}

const isSiteSettingsOpen = ref(false)
const selectedSiteForSettings = ref<UpstreamSite | null>(null)

const openSiteSettings = (site: UpstreamSite) => {
  selectedSiteForSettings.value = site
  isSiteSettingsOpen.value = true
}

const closeSiteSettings = () => {
  isSiteSettingsOpen.value = false
  selectedSiteForSettings.value = null
}

const onSiteSettingsSaved = (siteId: string, settings: { balanceThreshold: number | null }) => {
  const site = upstreamSites.value.find(s => s.id === siteId)
  if (site) {
    site.settings = settings
  }
}

const groupedGroups = computed<Record<string, UpstreamGroupInfo[]>>(() => {
  if (!selectedSiteForGroups.value) return {}
  const groups = selectedSiteForGroups.value.metrics.groups
  return groups.reduce<Record<string, UpstreamGroupInfo[]>>((acc, group) => {
    const platform = group.platform ?? t('admin.upstream.fields.unknownPlatform')
    if (!acc[platform]) acc[platform] = []
    acc[platform].push(group)
    return acc
  }, {})
})

const isGroupConnected = (group: UpstreamGroupInfo): boolean => {
  if (!selectedSiteForGroups.value) return false
  return isGroupConnectedForSite(selectedSiteForGroups.value.id, group)
}

const isGroupConnectedForSite = (siteId: string, group: UpstreamGroupInfo): boolean => (
  connectedGroupKeys.value.has(groupTargetKey(siteId, group.id)) ||
  connectedGroupKeys.value.has(groupTargetKey(siteId, group.name))
)

const connectedGroupsForSite = (site: UpstreamSite): UpstreamGroupInfo[] => (
  site.metrics.groups.filter(group => isGroupConnectedForSite(site.id, group))
)

const connectedGroupPreview = (site: UpstreamSite): string => {
  if (isLoadingGroupConnections.value) return t('admin.upstream.fields.checkingConnections')
  const groups = connectedGroupsForSite(site)
  return groups.length > 0
    ? t('admin.upstream.fields.connectedGroupsPreview', {
      groups: groups.map(group => `${group.name} · ${group.multiplierDisplay}`).join('、'),
    })
    : t('admin.upstream.fields.noConnectedGroups')
}

const downstreamConsumptionForSite = (site: UpstreamSite): DownstreamConsumptionItem | null => (
  downstreamConsumptionBySite.value.get(site.id) ?? null
)

const downstreamConsumptionAmount = (item: DownstreamConsumptionItem | null): string => (
  item?.amount === null || item?.amount === undefined ? '—' : `¥${item.amount.toFixed(2)}`
)

const downstreamConsumptionStatusLabel = (status: DownstreamConsumptionStatus): string => {
  switch (status) {
    case 'available':
      return t('admin.upstream.downstreamConsumption.available')
    case 'partial':
      return t('admin.upstream.downstreamConsumption.partial')
    case 'empty':
      return t('admin.upstream.downstreamConsumption.empty')
    case 'unsupported':
      return t('admin.upstream.downstreamConsumption.unsupported')
    default:
      return t('admin.upstream.downstreamConsumption.unavailable')
  }
}

const downstreamConsumptionRequestErrorLabel = (): string | null => {
  const errorKey = downstreamConsumptionError.value
  if (!errorKey) return null
  return te(errorKey) ? t(errorKey) : t('admin.upstream.downstreamConsumption.unavailable')
}

const downstreamConsumptionItemErrorLabel = (item: DownstreamConsumptionItem): string | null => {
  if (!item.errorKey) return null
  return te(item.errorKey) ? t(item.errorKey) : t('admin.upstream.downstreamConsumption.unavailable')
}

const downstreamConsumptionTooltip = (site: UpstreamSite, item: DownstreamConsumptionItem | null): string => {
  if (!item) {
    const requestError = downstreamConsumptionRequestErrorLabel()
    if (requestError) return requestError
    return connectedGroupsForSite(site).length > 0
      ? t('admin.upstream.downstreamConsumption.unavailable')
      : t('admin.upstream.downstreamConsumption.empty')
  }
  switch (item.status) {
    case 'available':
      return t('admin.upstream.downstreamConsumption.help')
    case 'partial':
      return [downstreamConsumptionStatusLabel(item.status), t('admin.upstream.downstreamConsumption.accountProgress', {
        success: item.successfulAccountCount,
        total: item.accountCount,
      }), downstreamConsumptionItemErrorLabel(item)].filter(Boolean).join(' · ')
    case 'empty':
      return t('admin.upstream.downstreamConsumption.empty')
    case 'unsupported':
      return t('admin.upstream.downstreamConsumption.unsupported')
    default:
      return downstreamConsumptionItemErrorLabel(item) ?? t('admin.upstream.downstreamConsumption.unavailable')
  }
}

const cnyMetricDisplay = (site: UpstreamSite, metric: UpstreamMetricValue): string | null => {
  if (metric.value === null || !Number.isFinite(metric.value) || site.rechargeRate <= 0 || !Number.isFinite(site.rechargeRate)) return null
  return t('admin.upstream.currency.cnyValue', { amount: (metric.value * site.rechargeRate).toFixed(2) })
}

const usdMetricDisplay = (metric: UpstreamMetricValue): string => {
  if (metric.display.toUpperCase().includes('USD')) return metric.display
  return t('admin.upstream.currency.usdValue', { amount: metric.display })
}

const lastUpdatedDisplay = (site: UpstreamSite): string => {
  if (!site.lastSyncedAt) return t('admin.upstream.fields.notSynced')
  const value = new Date(site.lastSyncedAt)
  if (Number.isNaN(value.getTime())) return t('admin.upstream.fields.notSynced')
  return new Intl.DateTimeFormat(locale.value, { dateStyle: 'medium', timeStyle: 'short' }).format(value)
}

onMounted(() => {
  void loadRefreshSettings()
  void loadConnectedGroupKeys()
  void loadDownstreamConsumption()
})

onBeforeUnmount(() => {
  stopCountdown()
})
</script>

<template>
  <div class="mx-auto w-full max-w-[1600px] space-y-6">
    <!-- Top Action Bar -->
    <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
      <div class="flex w-full flex-col gap-3 sm:flex-1">
        <div class="flex w-full flex-col gap-2 sm:flex-row sm:flex-wrap">
          <div class="relative w-full sm:w-80 sm:shrink-0">
            <Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <input
              v-model="searchQuery"
              name="upstreamSearch"
              type="text"
              :placeholder="t('admin.upstream.searchPlaceholder')"
              :aria-label="t('admin.upstream.searchPlaceholder')"
              autocomplete="off"
              spellcheck="false"
              class="h-10 w-full rounded-lg border border-border/50 bg-surface pl-10 pr-4 text-sm text-foreground outline-none transition-[color,background-color,border-color,box-shadow] placeholder:text-muted-foreground focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30"
            />
          </div>
          <div class="w-full sm:w-44 sm:shrink-0">
            <Select v-model="platformFilter" :aria-label="t('admin.upstream.filters.platform')">
              <option value="">{{ t('admin.upstream.filters.allPlatforms') }}</option>
              <option value="sub2api">{{ t('admin.upstream.modal.form.platforms.sub2api') }}</option>
              <option value="newapi">{{ t('admin.upstream.modal.form.platforms.newapi') }}</option>
            </Select>
          </div>
          <div class="w-full sm:w-48 sm:shrink-0">
            <Select v-model="connectedGroupTypeFilter" :aria-label="t('admin.upstream.filters.connectedGroupType')">
              <option value="">{{ t('admin.upstream.filters.allConnectedGroupTypes') }}</option>
              <option v-for="type in connectedGroupTypes" :key="type" :value="type">{{ type }}</option>
            </Select>
          </div>
          <div class="relative w-full sm:w-52 sm:shrink-0">
            <ArrowUpDown class="pointer-events-none absolute left-3 top-1/2 z-10 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Select v-model="sortMode" class="pl-9" :aria-label="t('admin.upstream.sort.label')">
              <option value="default">{{ t('admin.upstream.sort.default') }}</option>
              <option value="balanceDesc">{{ t('admin.upstream.sort.balanceDesc') }}</option>
              <option value="balanceAsc">{{ t('admin.upstream.sort.balanceAsc') }}</option>
              <option value="historyRechargeDesc">{{ t('admin.upstream.sort.historyRechargeDesc') }}</option>
              <option value="historyRechargeAsc">{{ t('admin.upstream.sort.historyRechargeAsc') }}</option>
            </Select>
          </div>
        </div>
        <p class="text-xs text-muted-foreground">
          {{ t('admin.upstream.summary', { connected: connectedCount, total: upstreamSites.length }) }}
        </p>
      </div>

      <div class="flex w-full flex-wrap items-center gap-2 sm:w-auto sm:justify-end">
        <div class="hidden md:flex h-10 items-center rounded-xl border border-border/50 bg-surface px-3 text-xs text-muted-foreground whitespace-nowrap">
          {{ countdownDisplay }}
        </div>
        <Button :disabled="isRefreshing" @click="runRefresh" variant="secondary" class="h-10 flex-1 gap-2 px-4 sm:flex-none">
          <Loader2 v-if="isRefreshing" class="w-4 h-4 animate-spin" />
          <RefreshCw v-else class="w-4 h-4" />
          {{ isRefreshing ? t('admin.upstream.refresh.refreshing') : t('admin.upstream.refresh.action') }}
        </Button>
        <Button @click="isAddModalOpen = true" class="h-10 flex-1 gap-2 px-4 shadow-sm sm:flex-none">
          <Plus class="w-4 h-4" />
          {{ t('admin.upstream.addSite') }}
        </Button>
      </div>
    </div>

    <!-- Table (List) View -->
    <div class="rounded-2xl border border-border/60 bg-card overflow-hidden shadow-sm">
      <div class="overflow-x-auto">
        <table class="w-full text-sm text-left">
          <thead class="bg-surface/50 text-muted-foreground border-b border-border/40">
            <tr>
              <th class="px-6 py-4 font-medium">{{ t('admin.upstream.fields.siteName') }}</th>
              <th class="px-6 py-4 font-medium">{{ t('admin.upstream.status.connected') }}</th>
              <th class="px-6 py-4 font-medium">{{ t('admin.upstream.fields.balance') }}</th>
              <th class="px-6 py-4 font-medium">{{ t('admin.upstream.fields.todayConsume') }}</th>
              <th class="px-6 py-4 font-medium">
                <Tooltip :text="t('admin.upstream.downstreamConsumption.help')" wide>
                  <span class="inline-flex items-center gap-1.5">
                    {{ t('admin.upstream.fields.downstreamConsume') }}
                    <CircleHelp class="h-3.5 w-3.5 text-muted-foreground" />
                  </span>
                </Tooltip>
              </th>
              <th class="px-6 py-4 font-medium">{{ t('admin.upstream.fields.historyRecharge') }}</th>
              <th class="px-6 py-4 font-medium">{{ t('admin.upstream.fields.connectedGroups') }}</th>
              <th class="px-6 py-4 font-medium text-right">{{ t('admin.upstream.action.actions') }}</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-border/40">
            <tr v-for="site in filteredSites" :key="site.id" class="hover:bg-surface/30 transition-colors">
              <td class="px-6 py-4">
                <div class="flex items-center gap-3">
                  <div :class="['w-8 h-8 rounded-lg flex items-center justify-center font-bold text-sm shrink-0', site.logoBg]">
                    {{ site.logo }}
                  </div>
                  <a :href="site.baseUrl" target="_blank" rel="noopener noreferrer" class="font-medium text-foreground hover:text-primary transition-colors truncate max-w-[150px] inline-block">
                    {{ site.name }}
                  </a>
                </div>
              </td>
              <td class="px-6 py-4">
                <div
                  v-if="siteSyncStates.get(site.id)?.phase && siteSyncStates.get(site.id)?.phase !== 'idle'"
                  class="inline-flex items-center gap-1.5 text-xs font-medium"
                  :class="{
                    'text-primary': siteSyncStates.get(site.id)?.phase === 'syncing',
                    'text-signal': siteSyncStates.get(site.id)?.phase === 'done',
                    'text-destructive': siteSyncStates.get(site.id)?.phase === 'error',
                  }"
                >
                  <Loader2 v-if="siteSyncStates.get(site.id)?.phase === 'syncing'" class="w-3.5 h-3.5 animate-spin" />
                  <CheckCircle2 v-else-if="siteSyncStates.get(site.id)?.phase === 'done'" class="w-3.5 h-3.5" />
                  <XCircle v-else class="w-3.5 h-3.5" />
                  <template v-if="siteSyncStates.get(site.id)?.phase === 'syncing'">{{ t('admin.upstream.syncStream.syncing') }}</template>
                  <template v-else-if="siteSyncStates.get(site.id)?.phase === 'done'">{{ t('admin.upstream.syncStream.done') }}</template>
                  <template v-else>{{ t('admin.upstream.syncStream.error') }}</template>
                </div>
                <div
                  v-else
                  class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium border"
                  :class="statusClasses[site.status]"
                >
                  <Loader2 v-if="site.status === 'connecting' || site.status === 'syncing'" class="w-3.5 h-3.5 animate-spin" />
                  <CheckCircle2 v-else-if="site.status === 'connected'" class="w-3.5 h-3.5" />
                  <PowerOff v-else-if="site.status === 'disabled'" class="w-3.5 h-3.5" />
                  <XCircle v-else class="w-3.5 h-3.5" />
                  {{ statusLabel(site.status) }}
                </div>
              </td>
              <td class="px-6 py-4">
                <div class="flex flex-col gap-0.5">
                  <span v-if="cnyMetricDisplay(site, site.metrics.balance)" class="font-medium text-primary">
                    {{ cnyMetricDisplay(site, site.metrics.balance) }}
                  </span>
                  <span :class="[cnyMetricDisplay(site, site.metrics.balance) ? 'text-xs font-medium text-primary/70' : 'font-medium text-primary']">
                    {{ usdMetricDisplay(site.metrics.balance) }}
                  </span>
                </div>
              </td>
              <td class="px-6 py-4">
                <div class="flex flex-col gap-0.5">
                  <span v-if="cnyMetricDisplay(site, site.metrics.todayConsume)" :class="['font-medium', site.metrics.todayConsume.value && site.metrics.todayConsume.value > 0 ? 'text-orange-500' : 'text-muted-foreground']">
                    {{ cnyMetricDisplay(site, site.metrics.todayConsume) }}
                  </span>
                  <span :class="[cnyMetricDisplay(site, site.metrics.todayConsume) ? 'text-xs font-medium' : 'font-medium', site.metrics.todayConsume.value && site.metrics.todayConsume.value > 0 ? (cnyMetricDisplay(site, site.metrics.todayConsume) ? 'text-orange-500/70' : 'text-orange-500') : 'text-muted-foreground']">
                    {{ usdMetricDisplay(site.metrics.todayConsume) }}
                  </span>
                </div>
              </td>
              <td class="min-w-[150px] px-6 py-4">
                <div v-if="isLoadingDownstreamConsumption" class="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Loader2 class="h-3.5 w-3.5 animate-spin" />
                  {{ t('admin.upstream.downstreamConsumption.loading') }}
                </div>
                <Tooltip v-else :text="downstreamConsumptionTooltip(site, downstreamConsumptionForSite(site))" wide>
                  <div class="flex flex-col gap-0.5">
                    <span
                      :class="[
                        'font-medium',
                        downstreamConsumptionForSite(site)?.amount !== null && downstreamConsumptionForSite(site)?.amount !== undefined
                          ? 'text-orange-500'
                          : 'text-muted-foreground',
                      ]"
                    >
                      {{ downstreamConsumptionAmount(downstreamConsumptionForSite(site)) }}
                    </span>
                    <span
                      v-if="downstreamConsumptionForSite(site)?.status === 'partial'"
                      class="text-xs font-medium text-warning"
                    >
                      {{ t('admin.upstream.downstreamConsumption.partial') }}
                    </span>
                  </div>
                </Tooltip>
              </td>
              <td class="px-6 py-4">
                <div class="flex flex-col gap-0.5">
                  <span v-if="cnyMetricDisplay(site, site.metrics.historyRecharge)" class="font-medium text-muted-foreground">
                    {{ cnyMetricDisplay(site, site.metrics.historyRecharge) }}
                  </span>
                  <span :class="[cnyMetricDisplay(site, site.metrics.historyRecharge) ? 'text-xs font-medium text-muted-foreground' : 'text-muted-foreground']">
                    {{ usdMetricDisplay(site.metrics.historyRecharge) }}
                  </span>
                </div>
              </td>
              <td class="min-w-[240px] px-6 py-4">
                <div v-if="isLoadingGroupConnections" class="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Loader2 class="h-3.5 w-3.5 animate-spin" />
                  {{ t('admin.upstream.fields.checkingConnections') }}
                </div>
                <div v-else-if="connectedGroupsForSite(site).length > 0" class="flex flex-wrap gap-1.5">
                  <span
                    v-for="group in connectedGroupsForSite(site)"
                    :key="`${site.id}-${group.name}`"
                    class="inline-flex max-w-full items-center rounded-md border border-primary/20 bg-primary/10 px-2 py-1 text-xs font-medium text-primary"
                    :title="`${group.name} · ${group.multiplierDisplay}`"
                  >
                    <span class="truncate">{{ group.name }} · {{ group.multiplierDisplay }}</span>
                  </span>
                </div>
                <span v-else class="text-xs text-muted-foreground">{{ t('admin.upstream.fields.noConnectedGroups') }}</span>
              </td>
              <td class="px-6 py-4 text-right">
                <div class="flex items-center justify-end gap-2">
                  <Tooltip v-if="site.metrics.groups.length > 0" :text="connectedGroupPreview(site)" wide>
                    <Button
                      variant="ghost"
                      class="h-8 px-2 text-xs text-primary hover:text-primary hover:bg-primary/10"
                      @click="openGroupsModal(site)"
                    >
                      {{ t('admin.upstream.fields.availableGroups') }}
                    </Button>
                  </Tooltip>
                  <Tooltip :text="syncingSiteIds.has(site.id) ? t('admin.upstream.action.syncing') : t('admin.upstream.action.sync')">
                    <button
                      class="p-1.5 rounded-md text-muted-foreground hover:bg-primary/10 hover:text-primary transition-colors"
                      :disabled="syncingSiteIds.has(site.id)"
                      @click="refreshSiteWithUsage(site.id)"
                    >
                      <Loader2 v-if="syncingSiteIds.has(site.id)" class="w-4 h-4 animate-spin" />
                      <RefreshCw v-else class="w-4 h-4" />
                    </button>
                  </Tooltip>
                  <Tooltip :text="t('admin.upstream.siteSettings.title')">
                    <button
                      class="p-1.5 rounded-md text-muted-foreground hover:bg-primary/10 hover:text-primary transition-colors"
                      @click="openSiteSettings(site)"
                    >
                      <Settings2 class="w-4 h-4" />
                    </button>
                  </Tooltip>
                  <Tooltip :text="t('admin.upstream.action.edit')">
                    <button
                      class="p-1.5 rounded-md text-muted-foreground hover:bg-primary/10 hover:text-primary transition-colors"
                      @click="handleEditSite(site)"
                    >
                      <Edit2 class="w-4 h-4" />
                    </button>
                  </Tooltip>
                  <Tooltip :text="t('admin.upstream.delete.action')">
                    <button
                      class="p-1.5 rounded-md text-muted-foreground hover:bg-red-500/10 hover:text-red-400 transition-colors"
                      @click="requestDeleteSite(site.id)"
                    >
                      <Trash2 class="w-4 h-4" />
                    </button>
                  </Tooltip>
                </div>
              </td>
            </tr>
            <tr v-if="filteredSites.length === 0">
              <td colspan="8" class="px-6 py-12 text-center text-muted-foreground">
                {{ t('admin.upstream.empty.description') }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Empty State -->
    <div v-if="filteredSites.length === 0" class="flex flex-col items-center justify-center py-12 text-center border border-dashed border-border/60 rounded-2xl bg-surface/30">
      <div class="w-12 h-12 rounded-full bg-muted/50 flex items-center justify-center mb-4">
        <Search class="w-6 h-6 text-muted-foreground" />
      </div>
      <p class="text-foreground font-medium">{{ t('admin.upstream.empty.title') }}</p>
      <p class="text-sm text-muted-foreground mt-1">{{ t('admin.upstream.empty.description') }}</p>
    </div>

    <!-- Delete Confirm Modal -->
    <div v-if="deletingSite" class="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div class="absolute inset-0 bg-background/80 backdrop-blur-sm" @click="cancelDeleteSite" />
      <div role="alertdialog" aria-modal="true" :aria-label="t('admin.upstream.delete.title')" class="relative w-full max-w-md overflow-hidden rounded-xl border border-border/70 border-t-2 border-t-destructive bg-card p-6 shadow-2xl">
        <div class="flex items-start gap-4">
          <div class="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border border-red-500/30 bg-red-500/10 text-red-400">
            <Trash2 class="h-5 w-5" />
          </div>
          <div class="min-w-0 flex-1">
            <h3 class="text-lg font-semibold text-foreground">{{ t('admin.upstream.delete.title') }}</h3>
            <p class="mt-2 text-sm leading-6 text-muted-foreground">
              {{ t('admin.upstream.delete.description', { name: deletingSite.name }) }}
            </p>
          </div>
        </div>

        <div v-if="deleteErrorKey" class="mt-5 flex items-start gap-2 rounded-xl border border-warning/30 bg-warning/10 px-3 py-2 text-sm text-warning">
          <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ t(deleteErrorKey) }}</span>
        </div>

        <div class="mt-6 flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
          <Button type="button" variant="secondary" @click="cancelDeleteSite">
            {{ t('admin.upstream.delete.cancel') }}
          </Button>
          <Button type="button" class="bg-red-500 text-white hover:bg-red-400" @click="confirmDeleteSite">
            {{ t('admin.upstream.delete.confirm') }}
          </Button>
        </div>
      </div>
    </div>

    <!-- Groups Modal -->
    <Teleport defer to="body">
      <div v-if="isGroupsModalOpen" class="fixed inset-0 z-[100] flex items-center justify-center p-4 sm:p-0">
        <!-- Backdrop -->
        <div
          class="absolute inset-0 bg-background/80 backdrop-blur-sm"
          @click="closeGroupsModal"
        ></div>

        <!-- Modal Content -->
        <div role="dialog" aria-modal="true" :aria-label="t('admin.upstream.fields.availableGroups')" class="relative max-h-[calc(100dvh-2rem)] w-full max-w-2xl overflow-hidden rounded-xl border border-border/60 border-t-2 border-t-primary bg-card shadow-2xl animate-in fade-in zoom-in-95 duration-200">

          <div class="flex items-center gap-3 border-b border-border/40 px-6 py-5">
            <h3 class="min-w-0 flex-1 text-lg font-semibold text-foreground">
              {{ t('admin.upstream.fields.availableGroups') }}
              <span class="text-muted-foreground ml-2 text-sm font-medium">{{ selectedSiteForGroups?.name }}</span>
            </h3>
            <span v-if="isLoadingGroupConnections" class="inline-flex shrink-0 items-center gap-1.5 text-xs text-muted-foreground">
              <Loader2 class="h-3.5 w-3.5 animate-spin" />
              {{ t('admin.upstream.fields.checkingConnections') }}
            </span>
            <button type="button" @click="closeGroupsModal" class="shrink-0 rounded-md p-1 text-muted-foreground transition-colors hover:bg-surface-elevated hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary" :aria-label="t('admin.upstream.fields.closeGroupsModal')">
              <X class="w-5 h-5" />
            </button>
          </div>

          <div class="max-h-[60dvh] space-y-6 overflow-y-auto p-6 overscroll-contain">
            <div v-for="(groups, platform) in groupedGroups" :key="platform" class="space-y-3">
              <h4 class="text-sm font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-2">
                <div class="w-1.5 h-1.5 rounded-full bg-primary"></div>
                {{ platform }}
              </h4>
              <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
                <button
                  v-for="group in groups"
                  :key="group.name"
                  type="button"
                  class="group relative flex flex-col items-center justify-center rounded-xl border p-3 text-center transition-colors"
                  :class="isGroupConnected(group)
                    ? 'border-primary/70 bg-primary/[0.10] ring-1 ring-primary/30 hover:border-primary hover:bg-primary/[0.14]'
                    : 'border-border/60 bg-surface/50 hover:border-primary/50 hover:bg-surface'"
                  :aria-label="`${group.name}${isGroupConnected(group) ? ` - ${t('admin.upstream.fields.connected')}` : ''}`"
                >
                  <span class="w-full truncate text-sm font-medium transition-colors" :class="isGroupConnected(group) ? 'text-primary' : 'text-foreground group-hover:text-primary'">{{ group.name }}</span>
                  <span v-if="isGroupConnected(group)" class="mt-1.5 inline-flex items-center gap-1 text-[10px] font-semibold text-primary">
                    <CheckCircle2 class="h-3 w-3" />
                    {{ t('admin.upstream.fields.connected') }}
                  </span>
                  <span
                    v-if="group.multiplier !== null && selectedSiteForGroups && selectedSiteForGroups.rechargeRate > 0"
                    class="mt-2 text-xs font-semibold text-primary px-2 py-0.5 rounded-md bg-primary/10 border border-primary/20"
                  >
                    {{ (group.multiplier * selectedSiteForGroups.rechargeRate).toFixed(2) }}
                  </span>
                  <template v-if="group.hasDedicatedMultiplier">
                    <Tooltip :text="t('admin.upstream.fields.dedicatedMultiplierTooltip')" wide>
                      <span class="text-[10px] text-muted-foreground mt-1">
                        {{ group.defaultMultiplierDisplay }} -&gt; {{ group.dedicatedMultiplierDisplay }}
                      </span>
                    </Tooltip>
                    <span class="mt-1 text-[9px] font-semibold text-accent px-1.5 py-0.5 rounded bg-accent/10 border border-accent/20">
                      {{ t('admin.upstream.fields.dedicatedMultiplierBadge') }}
                    </span>
                  </template>
                  <span v-else class="text-[10px] text-muted-foreground mt-1">
                    {{ group.multiplierDisplay }}
                  </span>
                </button>
              </div>
            </div>
          </div>

          <div class="p-4 border-t border-border/40 flex justify-end">
             <Button variant="ghost" @click="closeGroupsModal">{{ t('admin.upstream.fields.closeGroupsModal') }}</Button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- Add Site Modal -->
    <Teleport defer to="body">
      <div v-if="isAddModalOpen" class="fixed inset-0 z-[100] flex items-center justify-center p-4 sm:p-0">
        <!-- Backdrop -->
        <div
          class="absolute inset-0 bg-background/80 backdrop-blur-sm"
          @click="closeSiteModal"
        ></div>

        <!-- Modal Content -->
        <div role="dialog" aria-modal="true" :aria-label="t(editingSiteId ? 'admin.upstream.modal.editTitle' : 'admin.upstream.modal.title')" class="relative max-h-[calc(100dvh-2rem)] w-full max-w-2xl overflow-y-auto overscroll-contain rounded-xl border border-border/60 border-t-2 border-t-primary bg-card shadow-2xl animate-in fade-in zoom-in-95 duration-200">

          <div class="flex items-center justify-between px-6 py-5 border-b border-border/40">
            <h3 class="text-lg font-semibold text-foreground">
              {{ t(editingSiteId ? 'admin.upstream.modal.editTitle' : 'admin.upstream.modal.title') }}
            </h3>
            <button type="button" @click="closeSiteModal" class="rounded-md p-1 text-muted-foreground transition-colors hover:bg-surface-elevated hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary" :aria-label="t('admin.upstream.modal.cancel')">
              <X class="w-5 h-5" />
            </button>
          </div>

          <form @submit.prevent="handleAddSite" class="p-6">
            <div v-if="addErrorKey" class="mb-5 flex items-start gap-2 rounded-lg border border-destructive/20 bg-destructive/10 px-3 py-2 text-sm text-destructive" role="alert" aria-live="polite">
              <AlertCircle class="mt-0.5 h-4 w-4 shrink-0" />
              <span>{{ t(addErrorKey) }}</span>
            </div>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-5">
              <!-- Site Name -->
              <div class="space-y-2">
                <label for="upstream-site-name" class="text-sm font-medium text-foreground flex items-center gap-1">
                  <span class="text-red-500">*</span>
                  {{ t('admin.upstream.modal.form.siteName') }}
                </label>
                <Input
                  id="upstream-site-name"
                  v-model="newSiteForm.name"
                  name="siteName"
                  :placeholder="t('admin.upstream.modal.form.siteNamePlaceholder')"
                  :disabled="isAdding"
                  required
                  class="bg-surface border-border/50 focus:border-primary h-10"
                />
              </div>

              <!-- Platform Select -->
              <div class="space-y-2">
                <label for="upstream-site-platform" class="text-sm font-medium text-foreground flex items-center gap-1">
                  <span class="text-red-500">*</span>
                  {{ t('admin.upstream.modal.form.platform') }}
                </label>
                <Select
                  id="upstream-site-platform"
                  v-model="newSiteForm.platform"
                  name="platform"
                  :disabled="isAdding"
                >
                  <option value="auto">{{ t('admin.upstream.modal.form.platforms.auto') }}</option>
                  <option value="sub2api">{{ t('admin.upstream.modal.form.platforms.sub2api') }}</option>
                  <option value="newapi">{{ t('admin.upstream.modal.form.platforms.newapi') }}</option>
                </Select>
              </div>

              <!-- Site URL -->
              <div class="space-y-2 sm:col-span-2">
                <label for="upstream-site-url" class="text-sm font-medium text-foreground flex items-center gap-1">
                  <span class="text-red-500">*</span>
                  {{ t('admin.upstream.modal.form.siteUrl') }}
                </label>
                <Input
                  id="upstream-site-url"
                  v-model="newSiteForm.siteUrl"
                  name="siteUrl"
                  type="url"
                  :placeholder="t('admin.upstream.modal.form.siteUrlPlaceholder')"
                  :disabled="isAdding"
                  required
                  class="bg-surface border-border/50 focus:border-primary h-10"
                />
              </div>

              <!-- Auth Mode -->
              <div class="space-y-2 sm:col-span-2">
                <span class="text-sm font-medium text-foreground flex items-center gap-1">
                  <span class="text-red-500">*</span>
                  {{ t('admin.upstream.modal.form.authMode') }}
                </span>
                <div class="grid grid-cols-1 sm:grid-cols-2 gap-3" role="radiogroup" :aria-label="t('admin.upstream.modal.form.authMode')">
                  <label class="flex cursor-pointer items-start gap-3 rounded-xl border border-border/50 bg-surface p-3 text-sm transition-colors hover:border-primary/50">
                    <input v-model="newSiteForm.authMode" type="radio" value="password" :disabled="isAdding" class="mt-1" />
                    <span class="space-y-1">
                      <span class="block font-medium text-foreground">{{ t('admin.upstream.modal.form.authModes.password') }}</span>
                      <span class="block text-xs leading-5 text-muted-foreground">{{ t('admin.upstream.modal.form.authModes.passwordHelp') }}</span>
                    </span>
                  </label>
                  <label class="flex cursor-pointer items-start gap-3 rounded-xl border border-border/50 bg-surface p-3 text-sm transition-colors hover:border-primary/50">
                    <input v-model="newSiteForm.authMode" type="radio" :value="newSiteForm.platform === 'newapi' ? 'user_key' : 'token'" :disabled="isAdding" class="mt-1" />
                    <span class="space-y-1">
                      <span class="block font-medium text-foreground">{{ t(`admin.upstream.modal.form.authModes.${newSiteForm.platform === 'newapi' ? 'userKey' : 'token'}`) }}</span>
                      <span class="block text-xs leading-5 text-muted-foreground">{{ t(`admin.upstream.modal.form.authModes.${newSiteForm.platform === 'newapi' ? 'userKeyHelp' : 'tokenHelp'}`) }}</span>
                    </span>
                  </label>
                </div>
              </div>

              <!-- Account -->
              <div v-if="newSiteForm.authMode === 'password'" class="space-y-2">
                <label for="upstream-site-account" class="text-sm font-medium text-foreground flex items-center gap-1">
                  <span class="text-red-500">*</span>
                  {{ t('admin.upstream.modal.form.account') }}
                </label>
                <Input
                  id="upstream-site-account"
                  v-model="newSiteForm.account"
                  name="account"
                  :placeholder="t('admin.upstream.modal.form.accountPlaceholder')"
                  :disabled="isAdding"
                  required
                  class="bg-surface border-border/50 focus:border-primary h-10"
                />
              </div>

              <!-- Password -->
              <div v-if="newSiteForm.authMode === 'password'" class="space-y-2">
                <label for="upstream-site-password" class="text-sm font-medium text-foreground flex items-center gap-1">
                  <span v-if="!editingSiteId" class="text-red-500">*</span>
                  {{ t('admin.upstream.modal.form.password') }}
                </label>
                <Input
                  id="upstream-site-password"
                  v-model="newSiteForm.password"
                  name="password"
                  type="password"
                  :placeholder="t(editingSiteId ? 'admin.upstream.modal.form.passwordEditPlaceholder' : 'admin.upstream.modal.form.passwordPlaceholder')"
                  :disabled="isAdding"
                  :required="!editingSiteId"
                  class="bg-surface border-border/50 focus:border-primary h-10"
                />
                <p v-if="editingSiteId" class="text-xs leading-5 text-muted-foreground">
                  {{ t('admin.upstream.modal.form.passwordEditHelp') }}
                </p>
              </div>

              <template v-else-if="newSiteForm.authMode === 'token'">
                <div class="space-y-2 sm:col-span-2">
                  <label for="upstream-site-access-token" class="text-sm font-medium text-foreground flex items-center gap-1">
                    {{ t('admin.upstream.modal.form.accessToken') }}
                  </label>
                  <Input
                    v-model="newSiteForm.accessToken"
                    :placeholder="t('admin.upstream.modal.form.accessTokenPlaceholder')"
                    id="upstream-site-access-token"
                    name="accessToken"
                    :disabled="isAdding"
                    class="bg-surface border-border/50 focus:border-primary h-10"
                  />
                </div>
                <div class="space-y-2">
                  <label for="upstream-site-refresh-token" class="text-sm font-medium text-foreground flex items-center gap-1">
                    {{ t('admin.upstream.modal.form.refreshToken') }}
                  </label>
                  <Input
                    id="upstream-site-refresh-token"
                    v-model="newSiteForm.refreshToken"
                    name="refreshToken"
                    :placeholder="t('admin.upstream.modal.form.refreshTokenPlaceholder')"
                    :disabled="isAdding"
                    class="bg-surface border-border/50 focus:border-primary h-10"
                  />
                </div>
                <div class="space-y-2">
                  <label for="upstream-site-token-type" class="text-sm font-medium text-foreground flex items-center gap-1">
                    {{ t('admin.upstream.modal.form.tokenType') }}
                  </label>
                  <Input
                    id="upstream-site-token-type"
                    v-model="newSiteForm.tokenType"
                    name="tokenType"
                    :placeholder="t('admin.upstream.modal.form.tokenTypePlaceholder')"
                    :disabled="isAdding"
                    class="bg-surface border-border/50 focus:border-primary h-10"
                  />
                  <p class="text-xs leading-5 text-muted-foreground">
                    {{ t('admin.upstream.modal.form.tokenHelp') }}
                  </p>
                </div>
              </template>

              <template v-else>
                <div class="space-y-2">
                  <label for="upstream-site-user-id" class="text-sm font-medium text-foreground flex items-center gap-1">
                    <span class="text-red-500">*</span>
                    {{ t('admin.upstream.modal.form.userId') }}
                  </label>
                  <Input
                    id="upstream-site-user-id"
                    v-model="newSiteForm.userId"
                    name="userId"
                    :placeholder="t('admin.upstream.modal.form.userIdPlaceholder')"
                    :disabled="isAdding"
                    inputmode="numeric"
                    autocomplete="off"
                    required
                    class="bg-surface border-border/50 focus:border-primary h-10"
                  />
                </div>
                <div class="space-y-2">
                  <label for="upstream-site-user-key" class="text-sm font-medium text-foreground flex items-center gap-1">
                    <span class="text-red-500">*</span>
                    {{ t('admin.upstream.modal.form.userKey') }}
                  </label>
                  <Input
                    id="upstream-site-user-key"
                    v-model="newSiteForm.accessToken"
                    name="userKey"
                    type="password"
                    :placeholder="t('admin.upstream.modal.form.userKeyPlaceholder')"
                    :disabled="isAdding"
                    autocomplete="off"
                    required
                    class="bg-surface border-border/50 focus:border-primary h-10"
                  />
                  <p class="text-xs leading-5 text-muted-foreground">
                    {{ t('admin.upstream.modal.form.userKeyHelp') }}
                  </p>
                </div>
              </template>

              <!-- Recharge Rate -->
              <div class="space-y-2">
                <label for="upstream-site-recharge-rate" class="text-sm font-medium text-foreground flex items-center gap-1">
                  <span class="text-red-500">*</span>
                  {{ t('admin.upstream.modal.form.rechargeRate') }}
                </label>
                <input
                  id="upstream-site-recharge-rate"
                  v-model.number="newSiteForm.rechargeRate"
                  name="rechargeRate"
                  type="number"
                  min="0.000001"
                  step="0.000001"
                  :placeholder="t('admin.upstream.modal.form.rechargeRatePlaceholder')"
                  :disabled="isAdding"
                  required
                  class="h-10 w-full rounded-lg border border-border/50 bg-surface px-3 text-sm text-foreground outline-none transition-[color,background-color,border-color,box-shadow] placeholder:text-muted-foreground focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30 disabled:cursor-not-allowed disabled:opacity-50"
                />
                <p class="text-xs text-muted-foreground">
                  {{ t('admin.upstream.modal.form.rechargeRateHelp') }}
                </p>
              </div>

              <!-- Remark -->
              <div class="space-y-2">
                <label for="upstream-site-remark" class="ml-2.5 text-sm font-medium text-foreground">
                  {{ t('admin.upstream.modal.form.remark') }}
                </label>
                <Input
                  id="upstream-site-remark"
                  v-model="newSiteForm.remark"
                  name="remark"
                  :placeholder="t('admin.upstream.modal.form.remarkPlaceholder')"
                  :disabled="isAdding"
                  class="bg-surface border-border/50 focus:border-primary h-10"
                />
              </div>
            </div>

            <!-- Actions -->
            <div class="flex items-center justify-end gap-3 pt-4 border-t border-border/40 mt-6">
              <Button type="button" variant="ghost" :disabled="isAdding" @click="closeSiteModal" class="hover:bg-surface-line">
                {{ t('admin.upstream.modal.cancel') }}
              </Button>
              <Button type="submit" :disabled="isAdding" class="bg-primary text-primary-foreground hover:bg-primary/90">
                <Loader2 v-if="isAdding" class="h-4 w-4 animate-spin" />
              {{ isAdding ? t('admin.upstream.modal.submitting') : t(editingSiteId ? 'admin.upstream.modal.updateSubmit' : 'admin.upstream.modal.submit') }}
            </Button>
            </div>
          </form>
        </div>
      </div>
    </Teleport>

    <SiteSettingsModal
      :open="isSiteSettingsOpen"
      :site="selectedSiteForSettings"
      @close="closeSiteSettings"
      @saved="onSiteSettingsSaved"
    />
  </div>
</template>
