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
package run.ratchet.consumer.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Evaluates delivery, ownership, fencing, and retry invariants from durable evidence. */
public final class Oracle {
  public record JobKind(int failTimes, int maxRetries) {}

  public record Violation(String invariant, String jobId, String evidence) {}

  public static boolean terminal(String status) {
    return Set.of("SUCCEEDED", "FAILED", "CANCELED").contains(status == null ? "" : status);
  }

  public static List<Violation> evaluate(
      Set<String> submitted,
      Map<String, String> statuses,
      Ledger ledger,
      FaultTimeline timeline,
      List<String> quiescence) {
    var violations = new ArrayList<Violation>();
    for (String job : submitted) {
      if (!terminal(statuses.get(job)))
        violations.add(new Violation("I1", job, "not terminal: " + statuses.get(job)));
      var attempts = ledger.attempts().stream().filter(a -> a.jobId().equals(job)).toList();
      if ("SUCCEEDED".equals(statuses.get(job))
          && attempts.stream().noneMatch(a -> "ok".equals(a.outcome())))
        violations.add(new Violation("I2", job, "SUCCEEDED without an ok attempt"));
      var commits = ledger.commits().stream().filter(c -> c.jobId().equals(job)).toList();
      if (commits.size() > 1)
        violations.add(new Violation("I8", job, "multiple commits: " + commits));
      for (var commit : commits) {
        if (attempts.isEmpty()
            || !commit.incarnation().equals(attempts.get(attempts.size() - 1).incarnation()))
          violations.add(
              new Violation("I8", job, "committer differs from latest-started attempt: " + commit));
      }
      for (int i = 1; i < attempts.size(); i++) {
        var later = attempts.get(i);
        // An attempt stays exposed after its body ends until its incarnation commits the job, so a
        // fault in that gap legitimately causes a redelivery.
        boolean explained =
            "failed".equals(attempts.get(i - 1).outcome())
                || attempts.subList(0, i).stream()
                    .anyMatch(
                        a ->
                            timeline.entries().stream()
                                .anyMatch(
                                    f ->
                                        f.incarnation().equals(a.incarnation())
                                            && f.beforeUs() <= later.startedUs()
                                            && a.startedUs()
                                                <= (f.endedUs() == null
                                                    ? f.confirmedUs()
                                                    : f.endedUs())
                                            && committed(ledger, job, a.incarnation())
                                                >= f.beforeUs()));
        if (!explained)
          violations.add(new Violation("I3", job, "unexplained duplicate: " + later.attemptId()));
        for (int j = 0; j < i; j++) {
          var earlier = attempts.get(j);
          if (earlier.startedUs() < end(later, timeline)
              && later.startedUs() < end(earlier, timeline)) {
            boolean stale = stopped(earlier, timeline) || stopped(later, timeline);
            violations.add(
                new Violation(
                    stale ? "I4-stale" : "I4",
                    job,
                    "overlap: " + earlier.attemptId() + " and " + later.attemptId()));
          }
        }
      }
    }
    for (String evidence : quiescence) violations.add(new Violation("I9", null, evidence));
    return violations;
  }

  public static List<Violation> retryAccounting(
      Map<String, JobKind> kinds,
      Map<String, Integer> stored,
      Map<String, String> statuses,
      Ledger ledger,
      FaultTimeline timeline) {
    var violations = new ArrayList<Violation>();
    for (var entry : kinds.entrySet()) {
      String job = entry.getKey();
      // I1 already reports jobs that never finished; total_attempts is written only at the end.
      if (!terminal(statuses.get(job))) continue;
      var attempts = ledger.attempts().stream().filter(a -> a.jobId().equals(job)).toList();
      // JobTask.handleFailure increments once per retryable application failure, including the
      // exhausted failure (maxRetries + 1). Success does not increment. PostExecutionHandler
      // copies that count into the completion plan; SQL stores persist it as total_attempts.
      // DeadLetterService records DLQ events without another increment. Orphan recovery after
      // a crash preserves the count. These jobs use ordinary retryable application exceptions;
      // interrupted/incomplete bodies therefore do not contribute to this application-only rule.
      long expected = attempts.stream().filter(a -> "failed".equals(a.outcome())).count();
      // The increment commits after the ledger records "failed", so a fault in that gap can leave
      // a failure uncounted. Such failures widen the accepted range instead of failing the run.
      long uncertain =
          attempts.stream()
              .filter(a -> "failed".equals(a.outcome()) && exposed(a, attempts, ledger, timeline))
              .count();
      Integer actual = stored.get(job);
      if (actual == null || actual > expected || actual < expected - uncertain)
        violations.add(
            new Violation(
                "I10",
                job,
                "total_attempts="
                    + actual
                    + "; application failures="
                    + expected
                    + "; possibly uncounted="
                    + uncertain));
      boolean affected = attempts.stream().anyMatch(a -> exposed(a, attempts, ledger, timeline));
      if (!affected) {
        JobKind kind = entry.getValue();
        String status = kind.failTimes() <= kind.maxRetries() ? "SUCCEEDED" : "FAILED";
        if (!status.equals(statuses.get(job)))
          violations.add(
              new Violation(
                  "I10",
                  job,
                  "expected " + status + " for " + kind + "; observed " + statuses.get(job)));
      }
    }
    return violations;
  }

  // A failed attempt stays exposed until the job is claimed again, because the retry increment
  // must commit before the job becomes claimable. Any other attempt is exposed until its
  // incarnation commits the job.
  private static boolean exposed(
      Ledger.Attempt a, List<Ledger.Attempt> attempts, Ledger ledger, FaultTimeline timeline) {
    long exposedUntil =
        "failed".equals(a.outcome())
            ? attempts.stream()
                .mapToLong(Ledger.Attempt::startedUs)
                .filter(started -> started > a.startedUs())
                .min()
                .orElse(Long.MAX_VALUE)
            : committed(ledger, a.jobId(), a.incarnation());
    return timeline.entries().stream()
        .anyMatch(
            f ->
                f.incarnation().equals(a.incarnation())
                    && a.startedUs() <= f.confirmedUs()
                    && exposedUntil >= f.beforeUs());
  }

  private static long committed(Ledger ledger, String job, String incarnation) {
    return ledger.commits().stream()
        .filter(c -> c.jobId().equals(job) && c.incarnation().equals(incarnation))
        .mapToLong(Ledger.Commit::committedUs)
        .min()
        .orElse(Long.MAX_VALUE);
  }

  private static long end(Ledger.Attempt a, FaultTimeline timeline) {
    if (a.finishedUs() != null) return a.finishedUs();
    return timeline.entries().stream()
        .filter(
            f ->
                f.incarnation().equals(a.incarnation())
                    && (f.fault().equals("kill") || f.fault().equals("terminate"))
                    && f.confirmedUs() >= a.startedUs())
        .mapToLong(FaultTimeline.Entry::confirmedUs)
        .min()
        .orElse(Long.MAX_VALUE);
  }

  private static boolean stopped(Ledger.Attempt a, FaultTimeline timeline) {
    return timeline.entries().stream()
        .anyMatch(
            f ->
                f.fault().equals("stop")
                    && f.incarnation().equals(a.incarnation())
                    && f.beforeUs() < end(a, timeline)
                    && (f.endedUs() == null || f.endedUs() > a.startedUs()));
  }
}
