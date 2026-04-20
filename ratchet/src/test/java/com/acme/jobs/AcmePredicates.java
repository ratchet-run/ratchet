package com.acme.jobs;

import run.ratchet.api.JobResult;
import run.ratchet.api.SerializablePredicate;

public final class AcmePredicates {

  private AcmePredicates() {}

  public static SerializablePredicate<JobResult<Object>> successPredicate() {
    return JobResult::isSuccess;
  }
}
