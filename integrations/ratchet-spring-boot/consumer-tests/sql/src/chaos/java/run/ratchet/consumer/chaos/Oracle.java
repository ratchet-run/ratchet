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

/** Evaluates the P1 delivery and ownership invariants from durable evidence. */
public final class Oracle {
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
      for (int i = 1; i < attempts.size(); i++) {
        var later = attempts.get(i);
        // An attempt stays exposed after its body ends until its incarnation commits the job, so a
        // fault in that gap legitimately causes a redelivery.
        boolean explained =
            attempts.subList(0, i).stream()
                .anyMatch(
                    a ->
                        timeline.entries().stream()
                            .anyMatch(
                                f ->
                                    f.incarnation().equals(a.incarnation())
                                        && f.beforeUs() <= later.startedUs()
                                        && a.startedUs()
                                            <= (f.endedUs() == null ? f.confirmedUs() : f.endedUs())
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
