package run.ratchet.loadtest.api;

public class ResetResponse {

  private int runsReset;
  private int jobsDeleted;

  public ResetResponse() {}

  public ResetResponse(int runsReset, int jobsDeleted) {
    this.runsReset = runsReset;
    this.jobsDeleted = jobsDeleted;
  }

  public int getRunsReset() {
    return runsReset;
  }

  public void setRunsReset(int runsReset) {
    this.runsReset = runsReset;
  }

  public int getJobsDeleted() {
    return jobsDeleted;
  }

  public void setJobsDeleted(int jobsDeleted) {
    this.jobsDeleted = jobsDeleted;
  }
}
