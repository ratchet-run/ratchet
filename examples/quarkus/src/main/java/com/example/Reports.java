package com.example;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
public class Reports {
  private static final Logger LOG = Logger.getLogger(Reports.class);

  public void rebuild() {
    LOG.info("report rebuilt");
  }
}
