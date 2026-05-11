import React from 'react';
import styles from './styles.module.css';

type Status = 'yes' | 'no' | 'partial' | 'na';

interface Cell {
  status: Status;
  label?: string;
  footnote?: number;
}

interface Row {
  capability: string;
  cells: Cell[];
}

const products = ['Ratchet', 'Quartz', 'JobRunr', 'Spring Batch', 'jBeret', 'db-scheduler'];

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
      { status: 'partial', label: 'Planned' },
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
];

function CellIcon({status}: {status: Status}) {
  switch (status) {
    case 'yes':
      return <span aria-hidden="true">✓</span>;
    case 'no':
      return <span aria-hidden="true">✗</span>;
    case 'partial':
      return <span aria-hidden="true">●</span>;
    case 'na':
      return <span aria-hidden="true">—</span>;
  }
}

function statusLabel(status: Status): string {
  switch (status) {
    case 'yes': return 'Supported';
    case 'no': return 'Not supported';
    case 'partial': return 'Partial or caveat';
    case 'na': return 'Not applicable';
  }
}

export default function ComparisonMatrix() {
  return (
    <div className={styles.wrapper}>
      <div className={styles.scroll}>
        <table className={styles.matrix}>
          <thead>
            <tr>
              <th scope="col" className={styles.capabilityHeader}>Capability</th>
              {products.map((p) => (
                <th
                  key={p}
                  scope="col"
                  className={p === 'Ratchet' ? styles.ratchetHeader : styles.productHeader}
                >
                  {p}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.capability}>
                <th scope="row" className={styles.capability}>{row.capability}</th>
                {row.cells.map((cell, i) => (
                  <td
                    key={i}
                    className={`${styles.cell} ${styles[cell.status]}`}
                    aria-label={`${products[i]}: ${statusLabel(cell.status)}${cell.label ? ` (${cell.label})` : ''}`}
                  >
                    <span className={styles.icon}>
                      <CellIcon status={cell.status} />
                    </span>
                    {cell.label && <span className={styles.label}>{cell.label}</span>}
                    {cell.footnote && (
                      <sup className={styles.fn}>{cell.footnote}</sup>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className={styles.legend} role="list">
        <span className={styles.legendItem} role="listitem">
          <span className={`${styles.swatch} ${styles.yes}`} aria-hidden="true">✓</span>
          Supported
        </span>
        <span className={styles.legendItem} role="listitem">
          <span className={`${styles.swatch} ${styles.partial}`} aria-hidden="true">●</span>
          Partial / caveat
        </span>
        <span className={styles.legendItem} role="listitem">
          <span className={`${styles.swatch} ${styles.no}`} aria-hidden="true">✗</span>
          Not supported
        </span>
        <span className={styles.legendItem} role="listitem">
          <span className={`${styles.swatch} ${styles.na}`} aria-hidden="true">—</span>
          Not applicable
        </span>
      </div>
    </div>
  );
}
