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
import { LitElement, css } from 'lit';
import { JsonRpc } from 'jsonrpc';

export const ratchetSharedStyles = css`
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

  .empty {
    color: var(--lumo-secondary-text-color);
    padding: 18px 8px;
  }

  @media (max-width: 700px) {
    .page {
      padding: 8px;
    }
  }
`;

export class RatchetSnapshotElement extends LitElement {
  jsonRpc = new JsonRpc(this);

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

  _formatDate(value) {
    return value ? new Date(value).toLocaleString() : '-';
  }

  _errorMessage(error) {
    return error?.message || 'Ratchet snapshot is unavailable.';
  }
}
