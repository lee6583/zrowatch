<script setup lang="ts">
import {
  Comment,
  Fragment,
  Text,
  computed,
  nextTick,
  onBeforeUnmount,
  ref,
  useAttrs,
  useSlots,
  watch,
  type VNode,
} from 'vue'
import { Check, ChevronDown } from 'lucide-vue-next'
import { cn } from '@/lib/utils'

defineOptions({ inheritAttrs: false })

type SelectValue = string | number | undefined

interface SelectOption {
  value: SelectValue
  label: string
  disabled: boolean
}

const props = withDefaults(
  defineProps<{
    modelValue?: SelectValue
    modelModifiers?: {
      number?: boolean
      trim?: boolean
    }
    menuMinWidth?: number
    wrapOptions?: boolean
  }>(),
  {
    modelValue: '',
    menuMinWidth: 0,
    wrapOptions: false,
  },
)

const emit = defineEmits<{
  'update:modelValue': [value: SelectValue]
  change: [event: Event]
}>()

const attrs = useAttrs()
const slots = useSlots()
const triggerRef = ref<HTMLButtonElement | null>(null)
const menuRef = ref<HTMLDivElement | null>(null)
const open = ref(false)
const activeIndex = ref(-1)
const menuStyle = ref<Record<string, string>>({})
const listboxId = `select-listbox-${Math.random().toString(36).slice(2)}`

const attrValue = computed(() => attrs.value as SelectValue)
const selectedValue = computed(() => props.modelValue ?? attrValue.value ?? '')
const disabled = computed(() => attrs.disabled === true || attrs.disabled === '')
const name = computed(() => attrs.name as string | undefined)

const layoutClass = computed(() => (
  typeof attrs.class === 'string'
    ? attrs.class
      .split(/\s+/)
      .filter((item) => /^(?:[a-z0-9-]+:)*(?:w-|min-w-|max-w-|basis-|shrink|grow|flex-|self-|justify-self-|col-span-)/.test(item))
      .join(' ')
    : ''
))
const rootClass = computed(() => cn('relative inline-block w-full align-bottom', layoutClass.value))
const buttonAttrs = computed(() => {
  const {
    class: _class,
    value: _value,
    disabled: _disabled,
    name: _name,
    autocomplete: _autocomplete,
    ...rest
  } = attrs
  return rest
})

const textFromChildren = (children: VNode['children']): string => {
  if (typeof children === 'string') return children
  if (typeof children === 'number') return String(children)
  if (Array.isArray(children)) {
    return children.map((child) => {
      if (typeof child === 'string' || typeof child === 'number') return String(child)
      if (child && typeof child === 'object' && 'children' in child) return textFromChildren(child.children as VNode['children'])
      return ''
    }).join('')
  }
  return ''
}

const flattenOptionNodes = (nodes: VNode[]): VNode[] => {
  const result: VNode[] = []
  for (const node of nodes) {
    if (node.type === Comment || node.type === Text) continue
    if (node.type === Fragment && Array.isArray(node.children)) {
      result.push(...flattenOptionNodes(node.children as VNode[]))
      continue
    }
    result.push(node)
  }
  return result
}

const options = computed<SelectOption[]>(() => flattenOptionNodes(slots.default?.() ?? []).map((node) => {
  const props = node.props ?? {}
  const label = textFromChildren(node.children).replace(/\s+/g, ' ').trim()
  return {
    value: props.value as SelectValue ?? label,
    label,
    disabled: props.disabled === true || props.disabled === '',
  }
}))

const selectedOption = computed(() => options.value.find((option) => String(option.value ?? '') === String(selectedValue.value ?? '')))
const selectedLabel = computed(() => selectedOption.value?.label || options.value.find((option) => !option.disabled)?.label || '')

const updateMenuPosition = () => {
  const trigger = triggerRef.value
  if (!trigger) return
  const rect = trigger.getBoundingClientRect()
  const gap = 6
  const viewportPadding = 8
  const availableBelow = window.innerHeight - rect.bottom - viewportPadding - gap
  const availableAbove = rect.top - viewportPadding - gap
  const placeAbove = availableBelow < 180 && availableAbove > availableBelow
  const maxHeight = Math.max(160, Math.min(288, placeAbove ? availableAbove : availableBelow))
  const width = Math.min(
    Math.max(rect.width, props.menuMinWidth),
    window.innerWidth - viewportPadding * 2,
  )
  const left = Math.min(
    Math.max(viewportPadding, rect.left),
    window.innerWidth - viewportPadding - width,
  )
  menuStyle.value = {
    left: `${left}px`,
    width: `${width}px`,
    maxHeight: `${maxHeight}px`,
    ...(placeAbove
      ? { bottom: `${window.innerHeight - rect.top + gap}px` }
      : { top: `${rect.bottom + gap}px` }),
  }
}

const firstEnabledIndex = () => options.value.findIndex((option) => !option.disabled)

const selectedIndex = () => {
  const index = options.value.findIndex((option) => String(option.value ?? '') === String(selectedValue.value ?? ''))
  return index >= 0 && !options.value[index].disabled ? index : firstEnabledIndex()
}

const openMenu = async () => {
  if (disabled.value || open.value) return
  activeIndex.value = selectedIndex()
  updateMenuPosition()
  open.value = true
  await nextTick()
  updateMenuPosition()
}

const closeMenu = () => {
  open.value = false
}

const toggleMenu = () => {
  if (open.value) {
    closeMenu()
    return
  }
  void openMenu()
}

const normalizeValue = (value: SelectValue): SelectValue => {
  let nextValue = value
  if (props.modelModifiers?.trim && typeof nextValue === 'string') {
    nextValue = nextValue.trim()
  }
  if (props.modelModifiers?.number && typeof nextValue === 'string') {
    nextValue = Number(nextValue)
  }
  return nextValue
}

const emitChange = (value: SelectValue) => {
  const event = new Event('change')
  Object.defineProperty(event, 'target', { value: { value: String(value ?? '') } })
  emit('change', event)
}

const chooseOption = (option: SelectOption) => {
  if (option.disabled) return
  const nextValue = normalizeValue(option.value)
  emit('update:modelValue', nextValue)
  emitChange(nextValue)
  closeMenu()
  triggerRef.value?.focus()
}

const moveActive = (direction: 1 | -1) => {
  if (!open.value) {
    void openMenu()
    return
  }
  if (options.value.length === 0) return
  let next = activeIndex.value
  for (let i = 0; i < options.value.length; i++) {
    next = (next + direction + options.value.length) % options.value.length
    if (!options.value[next].disabled) {
      activeIndex.value = next
      return
    }
  }
}

const handleKeydown = (event: KeyboardEvent) => {
  switch (event.key) {
    case 'ArrowDown':
      event.preventDefault()
      moveActive(1)
      break
    case 'ArrowUp':
      event.preventDefault()
      moveActive(-1)
      break
    case 'Enter':
    case ' ':
      event.preventDefault()
      if (!open.value) {
        void openMenu()
      } else if (activeIndex.value >= 0) {
        chooseOption(options.value[activeIndex.value])
      }
      break
    case 'Escape':
      closeMenu()
      break
  }
}

const handleDocumentPointerDown = (event: PointerEvent) => {
  if (!open.value) return
  const target = event.target as Node
  if (triggerRef.value?.contains(target)) return
  if (menuRef.value?.contains(target)) return
  closeMenu()
}

const handleWindowUpdate = () => {
  if (open.value) updateMenuPosition()
}

watch(open, (isOpen) => {
  if (!isOpen) {
    window.removeEventListener('pointerdown', handleDocumentPointerDown)
    window.removeEventListener('resize', handleWindowUpdate)
    window.removeEventListener('scroll', handleWindowUpdate, true)
    return
  }
  window.addEventListener('pointerdown', handleDocumentPointerDown)
  window.addEventListener('resize', handleWindowUpdate)
  window.addEventListener('scroll', handleWindowUpdate, true)
})

watch(options, () => {
  if (open.value) activeIndex.value = selectedIndex()
})

onBeforeUnmount(() => {
  window.removeEventListener('pointerdown', handleDocumentPointerDown)
  window.removeEventListener('resize', handleWindowUpdate)
  window.removeEventListener('scroll', handleWindowUpdate, true)
})
</script>

<template>
  <span :class="rootClass">
    <input v-if="name" type="hidden" :name="name" :value="String(selectedValue ?? '')">
    <button
      v-bind="buttonAttrs"
      ref="triggerRef"
      type="button"
      :disabled="disabled"
      :aria-controls="listboxId"
      :aria-expanded="open"
      aria-haspopup="listbox"
      :class="cn(
        'flex h-11 w-full items-center justify-between gap-3 rounded-lg border border-border/70 bg-surface px-4 text-left text-sm text-foreground shadow-sm outline-none transition-[color,background-color,border-color,box-shadow,transform] hover:border-border hover:bg-surface-elevated focus-visible:border-primary focus-visible:ring-2 focus-visible:ring-primary/30 disabled:cursor-not-allowed disabled:opacity-60',
        attrs.class,
        open && 'border-primary ring-2 ring-primary/20',
      )"
      @click="toggleMenu"
      @keydown="handleKeydown"
    >
      <span class="min-w-0 flex-1 truncate">{{ selectedLabel }}</span>
      <ChevronDown class="h-4 w-4 shrink-0 text-muted-foreground transition-transform" :class="{ 'rotate-180': open }" aria-hidden="true" />
    </button>

    <Teleport to="body">
      <transition name="select-popover">
        <div
          v-if="open"
          :id="listboxId"
          ref="menuRef"
          role="listbox"
          :style="menuStyle"
          class="fixed z-[220] overflow-hidden rounded-lg border border-border/70 bg-surface-elevated p-1.5 shadow-2xl shadow-black/25 ring-1 ring-white/5 backdrop-blur-xl"
        >
          <div class="max-h-[inherit] overflow-y-auto overscroll-contain py-0.5">
            <button
              v-for="(option, index) in options"
              :key="`${String(option.value ?? '')}-${index}`"
              type="button"
              role="option"
              :aria-selected="String(option.value ?? '') === String(selectedValue ?? '')"
              :disabled="option.disabled"
              :class="cn(
                'flex min-h-9 w-full items-center gap-2 rounded-md px-2.5 py-2 text-left text-sm outline-none transition-colors',
                String(option.value ?? '') === String(selectedValue ?? '')
                  ? 'bg-primary text-primary-foreground'
                  : index === activeIndex
                    ? 'bg-surface-line text-foreground'
                    : 'text-foreground hover:bg-surface-line',
                option.disabled && 'cursor-not-allowed opacity-45',
              )"
              @mouseenter="activeIndex = index"
              @click="chooseOption(option)"
            >
              <Check
                class="h-4 w-4 shrink-0"
                :class="String(option.value ?? '') === String(selectedValue ?? '') ? 'opacity-100' : 'opacity-0'"
                aria-hidden="true"
              />
              <span
                :class="cn(
                  'min-w-0 flex-1',
                  props.wrapOptions ? 'whitespace-normal break-words' : 'truncate',
                )"
              >{{ option.label }}</span>
            </button>
          </div>
        </div>
      </transition>
    </Teleport>
  </span>
</template>

<style scoped>
.select-popover-enter-active,
.select-popover-leave-active {
  transition: opacity 0.12s ease, transform 0.12s ease;
}

.select-popover-enter-from,
.select-popover-leave-to {
  opacity: 0;
  transform: translateY(-4px) scale(0.98);
}
</style>
