package com.example;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import java.util.Set;
import run.ratchet.ri.security.PackagePrefixClassPolicy;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class JobsClassPolicy extends PackagePrefixClassPolicy {
  public JobsClassPolicy() {
    super(Set.of("com.example"));
  }
}
