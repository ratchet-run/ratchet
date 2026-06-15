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
package run.ratchet.loadtest.api;

public class StartRunRequest {

  private String workload = "noop";
  private int jobs = 1000;
  private long sleepMs = 5;
  private long sleepJitterMs;
  private double sleepSpikeRate;
  private long sleepSpikeMs;
  private double failureRate;
  private int payloadBytes;
  private int maxRetries;
  private String priority = "NORMAL";
  private long timeoutSeconds = 60;

  public String getWorkload() {
    return workload;
  }

  public void setWorkload(String workload) {
    this.workload = workload;
  }

  public int getJobs() {
    return jobs;
  }

  public void setJobs(int jobs) {
    this.jobs = jobs;
  }

  public long getSleepMs() {
    return sleepMs;
  }

  public void setSleepMs(long sleepMs) {
    this.sleepMs = sleepMs;
  }

  public long getSleepJitterMs() {
    return sleepJitterMs;
  }

  public void setSleepJitterMs(long sleepJitterMs) {
    this.sleepJitterMs = sleepJitterMs;
  }

  public double getSleepSpikeRate() {
    return sleepSpikeRate;
  }

  public void setSleepSpikeRate(double sleepSpikeRate) {
    this.sleepSpikeRate = sleepSpikeRate;
  }

  public long getSleepSpikeMs() {
    return sleepSpikeMs;
  }

  public void setSleepSpikeMs(long sleepSpikeMs) {
    this.sleepSpikeMs = sleepSpikeMs;
  }

  public double getFailureRate() {
    return failureRate;
  }

  public void setFailureRate(double failureRate) {
    this.failureRate = failureRate;
  }

  public int getPayloadBytes() {
    return payloadBytes;
  }

  public void setPayloadBytes(int payloadBytes) {
    this.payloadBytes = payloadBytes;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public String getPriority() {
    return priority;
  }

  public void setPriority(String priority) {
    this.priority = priority;
  }

  public long getTimeoutSeconds() {
    return timeoutSeconds;
  }

  public void setTimeoutSeconds(long timeoutSeconds) {
    this.timeoutSeconds = timeoutSeconds;
  }
}
