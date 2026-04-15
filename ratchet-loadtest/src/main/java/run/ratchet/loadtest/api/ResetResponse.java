package run.ratchet.loadtest.api;

public class ResetResponse {

  public int runsReset;
  public int jobsDeleted;

  public ResetResponse() {}

  public ResetResponse(int runsReset, int jobsDeleted) {
    this.runsReset = runsReset;
    this.jobsDeleted = jobsDeleted;
  }
}
