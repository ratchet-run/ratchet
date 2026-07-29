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

export class QwcRatchetCluster extends RatchetSnapshotElement {
  static styles = [
    ratchetSharedStyles,
    css`
    .summary {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }

    .node-id {
      display: inline-block;
      border-radius: 4px;
      padding: 2px 4px;
    }

    .local-node-id {
      background: var(--lumo-primary-color-10pct);
      color: var(--lumo-primary-text-color);
      font-weight: 600;
    }

    .badge {
      border: 1px solid var(--lumo-contrast-20pct);
      border-radius: 999px;
      padding: 2px 8px;
      font-size: var(--lumo-font-size-xs);
      white-space: nowrap;
    }

    .badge-active {
      color: var(--lumo-success-text-color);
      border-color: var(--lumo-success-color-50pct);
    }

    .badge-inactive {
      color: var(--lumo-secondary-text-color);
    }

    .badge-local {
      color: var(--lumo-primary-text-color);
      border-color: var(--lumo-primary-color-50pct);
    }

    @media (max-width: 700px) {
      th:nth-child(4),
      td:nth-child(4) {
        display: none;
      }
    }
    `
  ];

  render() {
    if (this._loading && !this._snapshot) {
      return html`<div class="page"><vaadin-progress-bar indeterminate></vaadin-progress-bar></div>`;
    }

    const snapshot = this._snapshot ?? { nodes: [], status: 'Waiting for Ratchet data.' };
    const nodes = snapshot.nodes ?? [];
    return html`
      <div class="page">
        <div class="toolbar">
          <div>
            <div class="title">Cluster</div>
            <div class="status">${this._error || snapshot.status}</div>
          </div>
          <vaadin-button title="Refresh" theme="tertiary small icon" @click="${this._loadSnapshot}">
            <vaadin-icon icon="font-awesome-solid:rotate"></vaadin-icon>
          </vaadin-button>
        </div>
        <div class="summary">
          <qui-badge small><span>${nodes.length} nodes</span></qui-badge>
          <qui-badge level="success" small><span>${nodes.filter((node) => node.active).length} active</span></qui-badge>
          <qui-badge level="contrast" small><span>${nodes.filter((node) => node.local).length} local</span></qui-badge>
        </div>
        ${this._renderNodes(nodes, snapshot.status)}
      </div>
    `;
  }

  _renderNodes(nodes, status) {
    if (!nodes.length) {
      return html`<div class="empty">No cluster nodes found. ${status}</div>`;
    }
    return html`
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Node</th>
              <th>Active</th>
              <th>Local</th>
              <th>Last Heartbeat</th>
            </tr>
          </thead>
          <tbody>
            ${nodes.map((node) => html`
              <tr>
                <td>
                  <code class="node-id ${node.local ? 'local-node-id' : ''}">${node.nodeId}</code>
                </td>
                <td>${this._renderActive(node.active)}</td>
                <td>${this._renderLocal(node.local)}</td>
                <td>${this._formatDate(node.lastHeartbeat)}</td>
              </tr>
            `)}
          </tbody>
        </table>
      </div>
    `;
  }

  _renderActive(active) {
    return active
      ? html`<span class="badge badge-active">Active</span>`
      : html`<span class="badge badge-inactive">Inactive</span>`;
  }

  _renderLocal(local) {
    return local
      ? html`<span class="badge badge-local">Local</span>`
      : html`<span class="badge badge-inactive">Remote</span>`;
  }
}

customElements.define('qwc-ratchet-cluster', QwcRatchetCluster);
