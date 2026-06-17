<script setup lang="ts">
type Status = 'yes' | 'no' | 'partial' | 'na'

interface Cell {
  status: Status
  label?: string
  footnote?: number
}

interface Row {
  capability: string
  cells: Cell[]
}

const products = ['Ratchet', 'Quartz', 'JobRunr', 'Spring Batch', 'jBeret', 'db-scheduler']

const rows: Row[] = [
  {
    capability: 'CDI-native (no Spring required)',
    cells: [
      { status: 'yes' },
      { status: 'no' },
      { status: 'partial', label: 'Partial', footnote: 1 },
      { status: 'no' },
      { status: 'yes' },
      { status: 'na', label: 'N/A', footnote: 2 },
    ],
  },
  {
    capability: 'Lambda / method-reference API',
    cells: [
      { status: 'yes' },
      { status: 'no' },
      { status: 'yes' },
      { status: 'no' },
      { status: 'no' },
      { status: 'partial', label: 'Limited' },
    ],
  },
  {
    capability: 'Persistent jobs',
    cells: [
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
    ],
  },
  {
    capability: 'Cron scheduling',
    cells: [
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'partial', label: 'via ext' },
      { status: 'yes' },
      { status: 'yes' },
    ],
  },
  {
    capability: 'Job chains / workflows',
    cells: [
      { status: 'yes' },
      { status: 'no', footnote: 3 },
      { status: 'partial', label: 'Limited', footnote: 5 },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'no' },
    ],
  },
  {
    capability: 'Multi-store (SQL + MongoDB)',
    cells: [
      { status: 'yes' },
      { status: 'partial', label: 'SQL only' },
      { status: 'yes' },
      { status: 'partial', label: 'SQL only' },
      { status: 'partial', label: 'SQL only' },
      { status: 'partial', label: 'SQL only' },
    ],
  },
  {
    capability: 'Pluggable store SPI + TCK',
    cells: [
      { status: 'yes' },
      { status: 'no' },
      { status: 'partial', label: 'Limited' },
      { status: 'no' },
      { status: 'no' },
      { status: 'no' },
    ],
  },
  {
    capability: 'Batch / streaming primitives',
    cells: [
      { status: 'yes' },
      { status: 'no' },
      { status: 'partial', label: 'Pro only', footnote: 4 },
      { status: 'yes', label: 'Native' },
      { status: 'yes' },
      { status: 'no' },
    ],
  },
  {
    capability: 'Built-in circuit breaker',
    cells: [
      { status: 'yes' },
      { status: 'no' },
      { status: 'no' },
      { status: 'no' },
      { status: 'no' },
      { status: 'no' },
    ],
  },
  {
    capability: 'Dead letter queue',
    cells: [
      { status: 'yes' },
      { status: 'no' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'no' },
    ],
  },
  {
    capability: 'Auto-recovery from crashes',
    cells: [
      { status: 'yes' },
      { status: 'partial', label: 'Clustered' },
      { status: 'yes' },
      { status: 'no' },
      { status: 'no' },
      { status: 'yes' },
    ],
  },
  {
    capability: 'Worker tag affinity',
    cells: [
      { status: 'yes' },
      { status: 'no' },
      { status: 'no' },
      { status: 'yes' },
      { status: 'no' },
      { status: 'no' },
    ],
  },
  {
    capability: 'Web dashboard',
    cells: [
      { status: 'no', label: 'By design', footnote: 6 },
      { status: 'no' },
      { status: 'yes' },
      { status: 'partial', label: 'Partial' },
      { status: 'no' },
      { status: 'yes' },
    ],
  },
  {
    capability: 'Spring Boot starter',
    cells: [
      { status: 'no', label: 'Unplanned' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes', label: 'Native' },
      { status: 'no' },
      { status: 'yes' },
    ],
  },
  {
    capability: 'Caller-principal capture (Jakarta Security)',
    cells: [
      { status: 'yes' },
      { status: 'no' },
      { status: 'no' },
      { status: 'no' },
      { status: 'no' },
      { status: 'no' },
    ],
  },
  {
    capability: 'Apache 2.0, no paid tier',
    cells: [
      { status: 'yes' },
      { status: 'yes' },
      { status: 'partial', label: 'Core only', footnote: 4 },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
    ],
  },
  {
    capability: 'Stable 1.0 release',
    cells: [
      { status: 'no', label: 'alpha' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
      { status: 'yes' },
    ],
  },
]

const iconFor = (s: Status) => ({ yes: '✓', no: '✗', partial: '●', na: '—' }[s])
const statusLabel = (s: Status) => ({
  yes: 'Supported',
  no: 'Not supported',
  partial: 'Partial or caveat',
  na: 'Not applicable',
}[s])
</script>

<template>
  <div class="cm-wrapper">
    <div
      class="cm-scroll"
      tabindex="0"
      role="region"
      aria-label="Feature comparison across job schedulers (scroll horizontally)"
    >
      <table class="cm-matrix">
        <thead>
          <tr>
            <th scope="col" class="cm-capability-header">Capability</th>
            <th
              v-for="(p, i) in products"
              :key="p"
              scope="col"
              :class="p === 'Ratchet' ? 'cm-ratchet-header' : 'cm-product-header'"
            >{{ p }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in rows" :key="row.capability">
            <th scope="row" class="cm-capability">{{ row.capability }}</th>
            <td
              v-for="(cell, i) in row.cells"
              :key="i"
              :class="['cm-cell', `cm-${cell.status}`]"
              :aria-label="`${products[i]}: ${statusLabel(cell.status)}${cell.label ? ' (' + cell.label + ')' : ''}`"
            >
              <span class="cm-icon">{{ iconFor(cell.status) }}</span>
              <span v-if="cell.label" class="cm-label">{{ cell.label }}</span>
              <sup v-if="cell.footnote" class="cm-fn">{{ cell.footnote }}</sup>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div class="cm-legend" role="list">
      <span class="cm-legend-item" role="listitem">
        <span class="cm-swatch cm-yes" aria-hidden="true">✓</span> Supported
      </span>
      <span class="cm-legend-item" role="listitem">
        <span class="cm-swatch cm-partial" aria-hidden="true">●</span> Partial / caveat
      </span>
      <span class="cm-legend-item" role="listitem">
        <span class="cm-swatch cm-no" aria-hidden="true">✗</span> Not supported
      </span>
      <span class="cm-legend-item" role="listitem">
        <span class="cm-swatch cm-na" aria-hidden="true">—</span> Not applicable
      </span>
    </div>
  </div>
</template>

<style scoped>
.cm-wrapper { margin: 1.5rem 0 2rem; }

.cm-scroll {
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}

.cm-matrix {
  width: 100%;
  border-collapse: separate;
  border-spacing: 4px;
  font-size: 0.92rem;
  min-width: 640px;
}

/* No padding reset here: a `.cm-matrix td` reset outranks the per-cell classes
   below and would flatten their padding to zero. The scoped cell classes
   already out-specify VitePress's default table padding, so they can own it. */

.cm-capability-header,
.cm-product-header {
  text-align: center;
  font-weight: 600;
  padding: 0.75rem 0.5rem;
  background: var(--vp-c-default-soft);
  color: var(--vp-c-text-1);
  border-radius: 6px;
}

.cm-capability-header {
  text-align: left;
  padding: 0.75rem 0.75rem;
  white-space: nowrap;
}

.cm-ratchet-header {
  text-align: center;
  padding: 0.75rem 0.5rem;
  border-radius: 6px;
  background: var(--vp-c-brand-1);
  font-weight: 700;
  /* Text colour is theme-keyed in custom.css (white on the light amber fill,
     near-black on the dark gold fill) to avoid the scoped light/dark tie. */
}

.cm-capability {
  text-align: left;
  font-weight: 500;
  padding: 0.85rem 0.9rem;
  background: var(--vp-c-default-soft);
  color: var(--vp-c-text-1);
  border-radius: 6px;
  vertical-align: middle;
}

.cm-cell {
  text-align: center;
  vertical-align: middle;
  padding: 0.85rem 0.7rem;
  border-radius: 6px;
  position: relative;
  font-weight: 600;
  line-height: 1.2;
  min-width: 4.5rem;
}

.cm-icon {
  display: block;
  font-size: 1.15rem;
  line-height: 1;
}

/* Labels and footnotes inherit the cell's status colour. They are small text,
   so they sit fully opaque on it (any opacity drops them below AA) and the
   status colours below are chosen to clear 4.5:1 against the blended cell
   background in both themes. */
.cm-label {
  display: block;
  font-size: 0.7rem;
  font-weight: 500;
  margin-top: 0.2rem;
}

.cm-fn {
  position: absolute;
  top: 2px;
  right: 4px;
  font-size: 0.65rem;
  color: inherit;
}

/* Status colours (cells + legend swatches) live in custom.css, keyed on
   html:not(.dark) / html.dark so light and dark are mutually exclusive. As
   scoped rules the dark overrides tied with the light rules on specificity and
   source order — not the theme — decided the winner. */

.cm-legend {
  display: flex;
  gap: 1.25rem;
  flex-wrap: wrap;
  margin-top: 1rem;
  padding: 0.75rem 1rem;
  background: var(--vp-c-default-soft);
  border-radius: 6px;
  font-size: 0.85rem;
}

.cm-legend-item {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
}

.cm-swatch {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 1.4rem;
  height: 1.4rem;
  border-radius: 4px;
  font-weight: 700;
  font-size: 0.9rem;
}

@media (max-width: 768px) {
  .cm-matrix { font-size: 0.82rem; }
  .cm-capability { padding: 0.5rem; }
  .cm-cell { padding: 0.4rem 0.25rem; min-width: 3.5rem; }
  .cm-icon { font-size: 0.95rem; }
  .cm-label { font-size: 0.62rem; }
}

/* Override default vp-doc table styles inside this component */
.cm-wrapper :deep(table) { background: transparent; }
</style>
