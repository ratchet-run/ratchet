/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { html, css } from 'lit';
import { RatchetSnapshotElement, ratchetSharedStyles } from './qwc-ratchet-base.js';
import '@vaadin/button';
import '@vaadin/icon';
import '@vaadin/progress-bar';
import 'qui-badge';

export class QwcRatchetJobs extends RatchetSnapshotElement {
  static styles = [
    ratchetSharedStyles,
    css`
    .health {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(112px, 1fr));
      gap: 8px;
    }

    .metric {
      border: 1px solid var(--lumo-contrast-10pct);
      border-radius: 6px;
      padding: 8px 10px;
      min-width: 0;
      background: var(--lumo-base-color);
    }

    .metric-label {
      color: var(--lumo-secondary-text-color);
      font-size: var(--lumo-font-size-xs);
      text-transform: uppercase;
    }

    .metric-value {
      display: block;
      font-size: var(--lumo-font-size-l);
      font-weight: 600;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }

    .detail-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      color: var(--lumo-secondary-text-color);
      font-size: var(--lumo-font-size-s);
    }

    .status-pill {
      border: 1px solid var(--lumo-contrast-20pct);
      border-radius: 999px;
      padding: 2px 8px;
      font-size: var(--lumo-font-size-xs);
      white-space: nowrap;
    }

    .status-succeeded {
      color: var(--lumo-success-text-color);
      border-color: var(--lumo-success-color-50pct);
    }

    .status-failed {
      color: var(--lumo-error-text-color);
      border-color: var(--lumo-error-color-50pct);
    }

    .status-running {
      color: var(--lumo-primary-text-color);
      border-color: var(--lumo-primary-color-50pct);
    }

    .status-paused,
    .status-waiting {
      color: var(--lumo-warning-text-color);
      border-color: var(--lumo-warning-color-50pct);
    }

    @media (max-width: 700px) {
      th:nth-child(1),
      td:nth-child(1) {
        width: 38%;
      }

      th:nth-child(4),
      td:nth-child(4),
      th:nth-child(5),
      td:nth-child(5) {
        display: none;
      }
    }
    `
  ];

  render() {
    if (this._loading && !this._snapshot) {
      return html`<div class="page"><vaadin-progress-bar indeterminate></vaadin-progress-bar></div>`;
    }

    const snapshot = this._snapshot ?? { jobs: [], health: null, status: 'Waiting for Ratchet data.' };
    return html`
      <div class="page">
        <div class="toolbar">
          <div>
            <div class="title">Jobs</div>
            <div class="status">${this._error || snapshot.status}</div>
          </div>
          <vaadin-button title="Refresh" theme="tertiary small icon" @click="${this._loadSnapshot}">
            <vaadin-icon icon="font-awesome-solid:rotate"></vaadin-icon>
          </vaadin-button>
        </div>
        ${this._renderHealth(snapshot.health)}
        ${this._renderJobs(snapshot.jobs ?? [], snapshot.status)}
      </div>
    `;
  }

  _renderHealth(health) {
    if (!health) {
      return html``;
    }
    return html`
      <div class="health">
        ${this._metric('Pending', health.pendingCount)}
        ${this._metric('Ready', health.readyCount)}
        ${this._metric('Running', health.runningCount)}
        ${this._metric('Waiting', health.waitingCount)}
        ${this._metric('Failed', health.failedCount)}
        ${this._metric('Succeeded', health.succeededCount)}
        ${this._metric('Canceled', health.canceledCount)}
        ${this._metric('Paused', health.pausedCount)}
        ${this._metric('Stuck', health.stuckCount)}
        ${this._metric('Retry Rate', this._formatPercent(health.retryRate))}
        ${this._metric('Avg Time', this._formatMillis(health.avgProcessingTimeMs))}
        ${this._metric('P95 Wait', this._formatMillis(health.p95QueueWaitMs))}
      </div>
      <div class="detail-row">
        <span>Oldest pending: ${this._formatDate(health.oldestPendingJobTime)}</span>
        ${this._renderCountBadges('Type', health.pendingByType)}
        ${this._renderCountBadges('Priority', health.pendingByPriority)}
      </div>
    `;
  }

  _renderJobs(jobs, status) {
    if (!jobs.length) {
      return html`<div class="empty">No jobs found. ${status}</div>`;
    }
    return html`
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Id</th>
              <th>Type</th>
              <th>Status</th>
              <th>Created</th>
              <th>Next Run</th>
            </tr>
          </thead>
          <tbody>
            ${jobs.map((job) => html`
              <tr>
                <td><code>${job.id}</code></td>
                <td>${job.type}</td>
                <td>${this._renderStatus(job.status)}</td>
                <td>${this._formatDate(job.createdAt)}</td>
                <td>${this._formatDate(job.nextRunAt)}</td>
              </tr>
            `)}
          </tbody>
        </table>
      </div>
    `;
  }

  _metric(label, value) {
    return html`
      <div class="metric">
        <span class="metric-label">${label}</span>
        <span class="metric-value">${value}</span>
      </div>
    `;
  }

  _renderStatus(status) {
    const cssClass = `status-${(status || '').toLowerCase()}`;
    return html`<span class="status-pill ${cssClass}">${status || 'UNKNOWN'}</span>`;
  }

  _renderCountBadges(label, values) {
    const entries = Object.entries(values || {});
    if (!entries.length) {
      return html``;
    }
    return html`
      <span>${label}:</span>
      ${entries.map(([key, value]) => html`<qui-badge small><span>${key}: ${value}</span></qui-badge>`)}
    `;
  }

  _formatMillis(value) {
    const number = Number(value || 0);
    return `${Math.round(number)} ms`;
  }

  _formatPercent(value) {
    const number = Number(value || 0);
    return `${Math.round(number * 100)}%`;
  }
}

customElements.define('qwc-ratchet-jobs', QwcRatchetJobs);
