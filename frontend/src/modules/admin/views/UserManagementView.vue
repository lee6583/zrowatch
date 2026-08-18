<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { AlertCircle, BellRing, ChevronLeft, ChevronRight, Loader2, Pencil, RefreshCw, Search, Trash2, WalletCards, X } from 'lucide-vue-next'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { deleteUserBalanceRule, listManagedUsers, saveUserBalanceRule } from '../api/userManagement'
import type { ManagedSub2APIUser, UserBalanceRuleInput } from '../types/userManagement'

const { t, te, locale } = useI18n()
const users = ref<ManagedSub2APIUser[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const totalPages = ref(1)
const searchDraft = ref('')
const search = ref('')
const status = ref('')
const role = ref('')
const loading = ref(false)
const errorKey = ref('')
const noticeKey = ref('')
const selectedUser = ref<ManagedSub2APIUser | null>(null)
const saving = ref(false)
const deleting = ref(false)
const form = ref<UserBalanceRuleInput>({
  warningEnabled: false, warningThreshold: null,
  autoRechargeEnabled: false, autoRechargeThreshold: null, autoRechargeAmount: null,
})

const hasRule = computed(() => Boolean(selectedUser.value?.rule))
const isNonNegativeNumber = (value: number | null): value is number => (
  typeof value === 'number' && Number.isFinite(value) && value >= 0
)
const canSave = computed(() => {
  if (!form.value.warningEnabled && !form.value.autoRechargeEnabled) return false
  if (form.value.warningEnabled && !isNonNegativeNumber(form.value.warningThreshold)) return false
  if (form.value.autoRechargeEnabled && (
    !isNonNegativeNumber(form.value.autoRechargeThreshold) ||
    typeof form.value.autoRechargeAmount !== 'number' || !Number.isFinite(form.value.autoRechargeAmount) || form.value.autoRechargeAmount <= 0
  )) return false
  return true
})

const displayError = computed(() => errorKey.value && te(errorKey.value) ? t(errorKey.value) : errorKey.value)
const timezone = () => Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'
const formatBalance = (value: number | null) => value == null ? t('admin.userManagement.common.placeholder') : value.toFixed(4)
const formatTime = (value?: string | null) => {
  if (!value) return t('admin.userManagement.common.placeholder')
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return t('admin.userManagement.common.placeholder')
  return new Intl.DateTimeFormat(locale.value, { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).format(date)
}
const userName = (user: ManagedSub2APIUser) => user.username || user.email || `#${user.id}`

const loadUsers = async () => {
  loading.value = true
  errorKey.value = ''
  try {
    const result = await listManagedUsers({
      page: page.value, pageSize: pageSize.value, status: status.value, role: role.value, search: search.value,
      sortBy: 'created_at', sortOrder: 'desc', timezone: timezone(),
    })
    users.value = result.items
    total.value = result.total
    page.value = result.page
    pageSize.value = result.pageSize
    totalPages.value = result.totalPages
  } catch (error) {
    errorKey.value = error instanceof Error ? error.message : 'admin.userManagement.errors.request'
  } finally {
    loading.value = false
  }
}

const submitSearch = () => { search.value = searchDraft.value.trim(); page.value = 1; void loadUsers() }
const changeFilter = () => { page.value = 1; void loadUsers() }
const changePage = (next: number) => { if (next < 1 || next > totalPages.value || next === page.value) return; page.value = next; void loadUsers() }

const openEditor = (user: ManagedSub2APIUser) => {
  selectedUser.value = user
  form.value = {
    warningEnabled: user.rule?.warningEnabled ?? false,
    warningThreshold: user.rule?.warningThreshold ?? null,
    autoRechargeEnabled: user.rule?.autoRechargeEnabled ?? false,
    autoRechargeThreshold: user.rule?.autoRechargeThreshold ?? null,
    autoRechargeAmount: user.rule?.autoRechargeAmount ?? null,
  }
  errorKey.value = ''
}
const closeEditor = () => { if (!saving.value && !deleting.value) selectedUser.value = null }

const saveRule = async () => {
  if (!selectedUser.value || !canSave.value) return
  saving.value = true
  errorKey.value = ''
  try {
    const rule = await saveUserBalanceRule(selectedUser.value.id, form.value)
    const row = users.value.find(item => item.id === selectedUser.value?.id)
    if (row) row.rule = rule
    selectedUser.value = null
    noticeKey.value = 'admin.userManagement.notices.saved'
  } catch (error) {
    errorKey.value = error instanceof Error ? error.message : 'admin.userManagement.errors.request'
  } finally {
    saving.value = false
  }
}

const removeRule = async () => {
  if (!selectedUser.value) return
  deleting.value = true
  errorKey.value = ''
  try {
    const id = selectedUser.value.id
    await deleteUserBalanceRule(id)
    const row = users.value.find(item => item.id === id)
    if (row) row.rule = null
    selectedUser.value = null
    noticeKey.value = 'admin.userManagement.notices.removed'
  } catch (error) {
    errorKey.value = error instanceof Error ? error.message : 'admin.userManagement.errors.request'
  } finally {
    deleting.value = false
  }
}

onMounted(loadUsers)
</script>

<template>
  <section class="flex min-h-0 flex-1 flex-col gap-4">
    <div class="flex flex-wrap items-end justify-between gap-3">
      <div>
        <h1 class="text-xl font-semibold text-foreground">{{ t('admin.userManagement.title') }}</h1>
        <p class="mt-1 text-sm text-muted-foreground">{{ t('admin.userManagement.subtitle') }}</p>
      </div>
      <Button variant="secondary" class="gap-2" :disabled="loading" @click="loadUsers">
        <RefreshCw class="h-4 w-4" :class="loading ? 'animate-spin' : ''" />
        {{ t('admin.userManagement.actions.refresh') }}
      </Button>
    </div>

    <div v-if="noticeKey" class="border-l-2 border-emerald-500 bg-emerald-500/5 px-4 py-3 text-sm text-emerald-700 dark:text-emerald-300">
      {{ t(noticeKey) }}
    </div>
    <div v-if="displayError && !selectedUser" class="flex items-center gap-2 border-l-2 border-rose-500 bg-rose-500/5 px-4 py-3 text-sm text-rose-700 dark:text-rose-300">
      <AlertCircle class="h-4 w-4 shrink-0" />{{ displayError }}
    </div>

    <div class="flex flex-wrap items-center gap-2 border-y border-border/60 py-3">
      <form class="relative w-full sm:w-72" @submit.prevent="submitSearch">
        <Search class="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input v-model="searchDraft" class="pl-9" :placeholder="t('admin.userManagement.filters.search')" />
      </form>
      <Select v-model="status" class="w-36" @change="changeFilter">
        <option value="">{{ t('admin.userManagement.filters.allStatuses') }}</option>
        <option value="active">{{ t('admin.userManagement.status.active') }}</option>
        <option value="disabled">{{ t('admin.userManagement.status.disabled') }}</option>
      </Select>
      <Select v-model="role" class="w-32" @change="changeFilter">
        <option value="">{{ t('admin.userManagement.filters.allRoles') }}</option>
        <option value="user">{{ t('admin.userManagement.roles.user') }}</option>
        <option value="admin">{{ t('admin.userManagement.roles.admin') }}</option>
      </Select>
      <span class="ml-auto text-sm text-muted-foreground">{{ t('admin.userManagement.total', { count: total }) }}</span>
    </div>

    <div class="min-h-0 overflow-auto border border-border/60 bg-card">
      <table class="w-full min-w-[980px] table-fixed text-sm">
        <thead class="sticky top-0 z-10 bg-muted/95 text-muted-foreground backdrop-blur">
          <tr>
            <th class="w-[25%] px-4 py-3 text-center font-medium">{{ t('admin.userManagement.table.user') }}</th>
            <th class="w-[10%] px-4 py-3 text-center font-medium">{{ t('admin.userManagement.table.status') }}</th>
            <th class="w-[12%] px-4 py-3 text-center font-medium">{{ t('admin.userManagement.table.balance') }}</th>
            <th class="w-[17%] px-4 py-3 text-center font-medium">{{ t('admin.userManagement.table.warning') }}</th>
            <th class="w-[18%] px-4 py-3 text-center font-medium">{{ t('admin.userManagement.table.recharge') }}</th>
            <th class="w-[12%] px-4 py-3 text-center font-medium">{{ t('admin.userManagement.table.checkedAt') }}</th>
            <th class="w-[6%] px-4 py-3 text-center font-medium">{{ t('admin.userManagement.table.actions') }}</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-border/50">
          <tr v-if="loading && users.length === 0"><td colspan="7" class="h-44 text-center text-muted-foreground"><Loader2 class="mx-auto mb-2 h-5 w-5 animate-spin" />{{ t('admin.userManagement.loading') }}</td></tr>
          <tr v-else-if="users.length === 0"><td colspan="7" class="h-44 text-center text-muted-foreground">{{ t('admin.userManagement.empty') }}</td></tr>
          <tr v-for="user in users" :key="user.id" class="hover:bg-muted/30">
            <td class="px-4 py-3 text-center"><div class="truncate font-medium text-foreground">{{ userName(user) }}</div><div class="truncate text-xs text-muted-foreground">{{ user.email || `ID ${user.id}` }}</div></td>
            <td class="px-4 py-3 text-center"><span class="inline-flex border px-2 py-1 text-xs" :class="user.status === 'active' ? 'border-emerald-500/30 text-emerald-600' : 'border-border text-muted-foreground'">{{ t(`admin.userManagement.status.${user.status === 'active' ? 'active' : 'disabled'}`) }}</span></td>
            <td class="px-4 py-3 text-center font-mono font-medium">{{ formatBalance(user.balance) }}</td>
            <td class="px-4 py-3 text-center"><span v-if="user.rule?.warningEnabled" :class="user.rule.warningActive ? 'text-rose-600' : 'text-foreground'">{{ formatBalance(user.rule.warningThreshold) }}</span><span v-else class="text-muted-foreground">{{ t('admin.userManagement.common.off') }}</span></td>
            <td class="px-4 py-3 text-center"><template v-if="user.rule?.autoRechargeEnabled"><div>{{ formatBalance(user.rule.autoRechargeThreshold) }} → +{{ formatBalance(user.rule.autoRechargeAmount) }}</div><div v-if="user.rule.rechargePending" class="text-xs text-amber-600">{{ t('admin.userManagement.states.retrying') }}</div></template><span v-else class="text-muted-foreground">{{ t('admin.userManagement.common.off') }}</span></td>
            <td class="px-4 py-3 text-center text-muted-foreground">{{ formatTime(user.rule?.lastCheckedAt) }}</td>
            <td class="px-4 py-3 text-center"><button class="inline-flex h-9 w-9 items-center justify-center text-muted-foreground hover:bg-muted hover:text-foreground" :title="t('admin.userManagement.actions.configure')" @click="openEditor(user)"><Pencil class="h-4 w-4" /></button></td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="flex items-center justify-end gap-2 text-sm">
      <button class="inline-flex h-9 w-9 items-center justify-center border border-border disabled:opacity-40" :disabled="page <= 1 || loading" @click="changePage(page - 1)"><ChevronLeft class="h-4 w-4" /></button>
      <span class="min-w-24 text-center text-muted-foreground">{{ page }} / {{ totalPages }}</span>
      <button class="inline-flex h-9 w-9 items-center justify-center border border-border disabled:opacity-40" :disabled="page >= totalPages || loading" @click="changePage(page + 1)"><ChevronRight class="h-4 w-4" /></button>
    </div>

    <div v-if="selectedUser" class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" @click.self="closeEditor">
      <div role="dialog" aria-modal="true" class="w-full max-w-lg border border-border bg-card shadow-xl">
        <div class="flex items-center justify-between border-b border-border px-5 py-4">
          <div><h2 class="font-semibold">{{ t('admin.userManagement.dialog.title') }}</h2><p class="mt-1 text-sm text-muted-foreground">{{ userName(selectedUser) }}</p></div>
          <button class="inline-flex h-9 w-9 items-center justify-center hover:bg-muted" @click="closeEditor"><X class="h-4 w-4" /></button>
        </div>
        <div class="space-y-5 p-5">
          <label class="flex items-center justify-between gap-4 border-b border-border/60 pb-4">
            <span class="flex items-center gap-2 font-medium"><BellRing class="h-4 w-4 text-amber-500" />{{ t('admin.userManagement.dialog.warningEnabled') }}</span>
            <input v-model="form.warningEnabled" type="checkbox" role="switch" class="h-5 w-5 accent-primary" />
          </label>
          <label v-if="form.warningEnabled" class="block space-y-2"><span class="text-sm text-muted-foreground">{{ t('admin.userManagement.dialog.warningThreshold') }}</span><input v-model.number="form.warningThreshold" type="number" min="0" step="0.0001" class="h-11 w-full border border-border bg-surface px-3 outline-none focus:border-primary" /></label>
          <label class="flex items-center justify-between gap-4 border-b border-border/60 pb-4">
            <span class="flex items-center gap-2 font-medium"><WalletCards class="h-4 w-4 text-emerald-500" />{{ t('admin.userManagement.dialog.rechargeEnabled') }}</span>
            <input v-model="form.autoRechargeEnabled" type="checkbox" role="switch" class="h-5 w-5 accent-primary" />
          </label>
          <div v-if="form.autoRechargeEnabled" class="grid grid-cols-2 gap-3">
            <label class="space-y-2"><span class="text-sm text-muted-foreground">{{ t('admin.userManagement.dialog.rechargeThreshold') }}</span><input v-model.number="form.autoRechargeThreshold" type="number" min="0" step="0.0001" class="h-11 w-full border border-border bg-surface px-3 outline-none focus:border-primary" /></label>
            <label class="space-y-2"><span class="text-sm text-muted-foreground">{{ t('admin.userManagement.dialog.rechargeAmount') }}</span><input v-model.number="form.autoRechargeAmount" type="number" min="0.0001" step="0.0001" class="h-11 w-full border border-border bg-surface px-3 outline-none focus:border-primary" /></label>
          </div>
          <div v-if="displayError" class="flex items-center gap-2 text-sm text-rose-600"><AlertCircle class="h-4 w-4" />{{ displayError }}</div>
        </div>
        <div class="flex items-center justify-between border-t border-border px-5 py-4">
          <Button v-if="hasRule" variant="ghost" class="gap-2 text-rose-600" :disabled="saving || deleting" @click="removeRule"><Loader2 v-if="deleting" class="h-4 w-4 animate-spin" /><Trash2 v-else class="h-4 w-4" />{{ t('admin.userManagement.actions.remove') }}</Button><span v-else />
          <div class="flex gap-2"><Button variant="secondary" :disabled="saving || deleting" @click="closeEditor">{{ t('admin.userManagement.actions.cancel') }}</Button><Button :disabled="!canSave || saving || deleting" class="gap-2" @click="saveRule"><Loader2 v-if="saving" class="h-4 w-4 animate-spin" />{{ t('admin.userManagement.actions.save') }}</Button></div>
        </div>
      </div>
    </div>
  </section>
</template>
