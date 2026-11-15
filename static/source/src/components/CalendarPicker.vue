<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

const props = defineProps({
  modelValue: { type: String, default: '' },
  placeholder: { type: String, default: 'YYYY-MM-DD' },
})
const emit = defineEmits(['update:modelValue'])

const root = ref(null)
const open = ref(false)
const text = ref(props.modelValue || '')
const viewYear = ref(new Date().getFullYear())
const viewMonth = ref(new Date().getMonth())

const weekdays = ['日', '一', '二', '三', '四', '五', '六']

function parse(value) {
  const matched = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value || '').trim())
  if (!matched) return null
  const year = Number(matched[1])
  const month = Number(matched[2]) - 1
  const day = Number(matched[3])
  const date = new Date(year, month, day)
  if (date.getFullYear() !== year || date.getMonth() !== month || date.getDate() !== day) {
    return null
  }
  return date
}

function format(date) {
  const y = date.getFullYear()
  const m = String(date.getMonth() + 1).padStart(2, '0')
  const d = String(date.getDate()).padStart(2, '0')
  return `${y}-${m}-${d}`
}

function syncView() {
  const date = parse(props.modelValue) || new Date()
  viewYear.value = date.getFullYear()
  viewMonth.value = date.getMonth()
}

function monthTitle(year, month) {
  return `${year}年${month + 1}月`
}

function monthCells(year, month) {
  const first = new Date(year, month, 1)
  const lead = first.getDay()
  const days = new Date(year, month + 1, 0).getDate()
  const cells = Array.from({ length: lead }, () => null)
  for (let day = 1; day <= days; day += 1) cells.push(day)
  while (cells.length % 7 !== 0) cells.push(null)
  return cells
}

const months = computed(() => {
  const second = new Date(viewYear.value, viewMonth.value + 1, 1)
  return [
    { year: viewYear.value, month: viewMonth.value, cells: monthCells(viewYear.value, viewMonth.value) },
    { year: second.getFullYear(), month: second.getMonth(), cells: monthCells(second.getFullYear(), second.getMonth()) },
  ]
})

const todayKey = format(new Date())

function pick(year, month, day) {
  const value = format(new Date(year, month, day))
  text.value = value
  emit('update:modelValue', value)
  open.value = false
}

function chooseOffset(kind) {
  const now = new Date()
  let date = new Date(now.getFullYear(), now.getMonth(), now.getDate())
  if (kind === 'yesterday') date.setDate(date.getDate() - 1)
  if (kind === '7') date.setDate(date.getDate() - 7)
  if (kind === '30') date.setDate(date.getDate() - 30)
  if (kind === 'month') date = new Date(now.getFullYear(), now.getMonth(), 1)
  if (kind === 'lastMonth') date = new Date(now.getFullYear(), now.getMonth() - 1, 1)
  text.value = format(date)
  emit('update:modelValue', text.value)
  viewYear.value = date.getFullYear()
  viewMonth.value = date.getMonth()
  open.value = false
}

function shiftMonth(delta) {
  const next = new Date(viewYear.value, viewMonth.value + delta, 1)
  viewYear.value = next.getFullYear()
  viewMonth.value = next.getMonth()
}

function commitText() {
  const date = parse(text.value)
  if (!date) {
    text.value = props.modelValue || ''
    return
  }
  const value = format(date)
  text.value = value
  emit('update:modelValue', value)
  viewYear.value = date.getFullYear()
  viewMonth.value = date.getMonth()
}

function toggle() {
  if (!open.value) syncView()
  open.value = !open.value
}

function onDocumentClick(event) {
  if (root.value && !root.value.contains(event.target)) open.value = false
}

watch(() => props.modelValue, (value) => {
  text.value = value || ''
})

onMounted(() => document.addEventListener('click', onDocumentClick))
onBeforeUnmount(() => document.removeEventListener('click', onDocumentClick))
</script>

<template>
  <div ref="root" class="picker">
    <div class="trigger">
      <input
        v-model="text"
        :placeholder="placeholder"
        @focus="toggle"
        @change="commitText"
        @blur="commitText"
      />
      <button type="button" class="icon" @click.stop="toggle">📅</button>
    </div>

    <div v-if="open" class="pop" @click.stop>
      <aside class="quick">
        <button @click="chooseOffset('today')">今天</button>
        <button @click="chooseOffset('yesterday')">昨天</button>
        <button @click="chooseOffset('7')">近 7 天</button>
        <button @click="chooseOffset('30')">近 30 天</button>
        <button @click="chooseOffset('month')">本月</button>
        <button @click="chooseOffset('lastMonth')">上月</button>
        <button @click="commitText(); open = false">自定义</button>
      </aside>

      <div class="calendar">
        <div class="nav">
          <button type="button" class="nav-btn" @click="shiftMonth(-1)">‹</button>
          <b>{{ monthTitle(months[0].year, months[0].month) }}</b>
          <b class="second-title">{{ monthTitle(months[1].year, months[1].month) }}</b>
          <button type="button" class="nav-btn" @click="shiftMonth(1)">›</button>
        </div>

        <div class="months">
          <div v-for="(item, index) in months" :key="index" class="month">
            <div class="week">
              <span v-for="day in weekdays" :key="day">{{ day }}</span>
            </div>
            <div class="days">
              <button
                v-for="(day, cellIndex) in item.cells"
                :key="cellIndex"
                type="button"
                :class="[
                  'day',
                  {
                    empty: !day,
                    today: day && format(new Date(item.year, item.month, day)) === todayKey,
                    selected: day && format(new Date(item.year, item.month, day)) === modelValue,
                  },
                ]"
                :disabled="!day"
                @click="day && pick(item.year, item.month, day)"
              >
                {{ day || '' }}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.picker {
  position: relative;
  display: inline-flex;
  flex-direction: column;
}

.trigger {
  display: flex;
  align-items: center;
  gap: 4px;
}

input {
  width: 150px;
  padding: 8px 10px;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 7px;
  font-size: 14px;
}

.icon {
  width: 32px;
  height: 34px;
  padding: 0;
  color: var(--text);
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 7px;
}

.pop {
  position: absolute;
  z-index: 100;
  top: calc(100% + 6px);
  left: 0;
  display: flex;
  gap: 14px;
  padding: 14px;
  background: var(--bg-soft);
  border: 1px solid var(--border);
  border-radius: 12px;
  box-shadow: 0 18px 50px rgba(0, 0, 0, 0.35);
}

.quick {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 104px;
  padding-right: 12px;
  border-right: 1px solid var(--border);
}

.quick button {
  padding: 8px 10px;
  color: var(--text);
  font-size: 14px;
  text-align: left;
  background: transparent;
  border: 0;
  border-radius: 6px;
}

.quick button:hover {
  background: var(--bg);
}

.calendar {
  width: min(560px, 70vw);
}

.nav {
  display: flex;
  align-items: center;
  gap: 18px;
  padding: 0 6px 8px;
}

.nav b {
  font-size: 15px;
  font-weight: 600;
}

.second-title {
  flex: 1;
}

.nav-btn {
  width: 28px;
  height: 28px;
  padding: 0;
  color: var(--text);
  background: transparent;
  border: 1px solid var(--border);
  border-radius: 7px;
}

.months {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 18px;
}

.week,
.days {
  display: grid;
  grid-template-columns: repeat(7, 1fr);
  gap: 3px;
}

.week span {
  padding: 4px 0;
  color: var(--muted);
  font-size: 12px;
  text-align: center;
}

.day {
  height: 32px;
  padding: 0;
  color: var(--text);
  font-size: 13px;
  background: transparent;
  border: 1px solid transparent;
  border-radius: 7px;
}

.day:hover:not(:disabled) {
  background: var(--bg);
  border-color: var(--border);
}

.day.empty {
  visibility: hidden;
}

.day.today {
  color: var(--primary);
  border-color: var(--primary);
}

.day.selected {
  color: #fff;
  background: var(--primary);
  border-color: var(--primary);
}

@media (max-width: 760px) {
  .pop {
    flex-direction: column;
  }
  .quick {
    flex-direction: row;
    flex-wrap: wrap;
    border-right: 0;
    border-bottom: 1px solid var(--border);
    padding: 0 0 8px;
  }
  .months,
  .calendar {
    width: 100%;
  }
}
</style>
