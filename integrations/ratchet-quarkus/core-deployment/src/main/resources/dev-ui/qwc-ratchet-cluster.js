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
import { LitElement, html, css } from 'lit';
import { JsonRpc } from 'jsonrpc';
import '@vaadin/button';
import '@vaadin/icon';
import '@vaadin/progress-bar';
import 'qui-badge';

export class QwcRatchetCluster extends LitElement {
  jsonRpc = new JsonRpc(this);

  static styles = css`
    :host {
      display: block;
      height: 100%;
      color: var(--lumo-body-text-color);
    }

    .page {
      display: flex;
      flex-direction: column;
      gap: 14px;
      height: 100%;
      padding: 12px;
      box-sizing: border-box;
    }

    .toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      border-bottom: 1px solid var(--lumo-contrast-10pct);
      padding-bottom: 10px;
    }

    .title {
      font-size: var(--lumo-font-size-l);
      font-weight: 600;
    }

    .status {
      color: var(--lumo-secondary-text-color);
      font-size: var(--lumo-font-size-s);
      margin-top: 3px;
    }

    .summary {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }

    .table-wrap {
      min-height: 0;
      overflow: auto;
      border-top: 1px solid var(--lumo-contrast-10pct);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }

    th {
      color: var(--lumo-secondary-text-color);
      font-size: var(--lumo-font-size-xs);
      font-weight: 600;
      text-align: left;
      text-transform: uppercase;
      padding: 9px 8px;
      border-bottom: 1px solid var(--lumo-contrast-10pct);
    }

    td {
      padding: 9px 8px;
      border-bottom: 1px solid var(--lumo-contrast-5pct);
      vertical-align: middle;
      overflow-wrap: anywhere;
    }

    code {
      font-size: var(--lumo-font-size-xs);
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

    .empty {
      color: var(--lumo-secondary-text-color);
      padding: 18px 8px;
    }

    @media (max-width: 700px) {
      .page {
        padding: 8px;
      }

      th:nth-child(4),
      td:nth-child(4) {
        display: none;
      }
    }
  `;

  static properties = {
    _snapshot: { state: true },
    _loading: { state: true },
    _error: { state: true },
    _streamObserver: { state: false }
  };

  constructor() {
    super();
    this._snapshot = null;
    this._loading = true;
    this._error = null;
    this._streamObserver = null;
  }

  connectedCallback() {
    super.connectedCallback();
    this._loadSnapshot();
    this._startStream();
  }

  disconnectedCallback() {
    this._cancelStream();
    super.disconnectedCallback();
  }

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

  _startStream() {
    this._cancelStream();
    this._streamObserver = this.jsonRpc.streamSnapshot()
      .onNext((jsonRpcResponse) => this._applySnapshot(jsonRpcResponse.result))
      .onError((error) => {
        this._error = this._errorMessage(error);
        this._loading = false;
      });
  }

  _cancelStream() {
    if (this._streamObserver) {
      this._streamObserver.cancel();
      this._streamObserver = null;
    }
  }

  _loadSnapshot() {
    this._loading = true;
    this.jsonRpc.getSnapshot()
      .then((jsonRpcResponse) => this._applySnapshot(jsonRpcResponse.result))
      .catch((error) => {
        this._error = this._errorMessage(error);
        this._loading = false;
      });
  }

  _applySnapshot(snapshot) {
    this._snapshot = snapshot;
    this._error = null;
    this._loading = false;
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

  _formatDate(value) {
    return value ? new Date(value).toLocaleString() : '-';
  }

  _errorMessage(error) {
    return error?.message || 'Ratchet snapshot is unavailable.';
  }
}

customElements.define('qwc-ratchet-cluster', QwcRatchetCluster);
