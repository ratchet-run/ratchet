---
layout: home

hero:
  name: "Ratchet"
  text: "Inject one service. Submit a method reference."
  tagline: "<span class='tag-full'>Persistent, retrying background jobs on plain Jakarta EE. No proprietary dependencies, no message broker, and they run unchanged on whichever application server you already use.</span><span class='tag-mobile'>Persistent, retrying background jobs on plain Jakarta EE. No broker, no proprietary deps.</span>"
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started/introduction
    - theme: alt
      text: API Reference
      link: /api-reference/overview

features:
  - title: CDI-native API
    details: Inject JobSchedulerService like any other bean and hand it a method reference. Ratchet persists it, runs it on exactly one node, retries on failure, and fires lifecycle events you can observe.
  - title: Resilience built in
    details: Retries with fixed or exponential backoff, a circuit breaker for flaky downstreams, a dead-letter queue, and pause, resume, and cancel at runtime. It's part of the scheduler contract, not a library you bolt on.
  - title: Override any default
    details: "Every default is a bean you can replace. Encrypt payloads with your own KMS, or swap the cluster coordinator, retry policy, or store with a standard @Alternative. It's plain CDI, not a plugin system you have to learn."
  - title: Native-image jobs on Quarkus
    details: "The same persistent, retrying jobs run on Quarkus, on the JVM and as a GraalVM native image. The extension provisions a dev database, supplies Ratchet's persistence unit, and registers the native metadata. Quarkus 3.20+, SQL or MongoDB."
  - title: Query and report
    details: "Read job and cluster state through a typed query service. Filter by principal, page large result sets, and build operational views without reaching into the store."
  - title: Runs where you run
    details: "The same job code runs unchanged on WildFly, Open Liberty, Payara, and GlassFish. A published conformance matrix shows what passes on each server and database. No rewrite, no broker, no lock-in."
---
