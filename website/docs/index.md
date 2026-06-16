---
layout: home

hero:
  name: "Ratchet"
  text: "Inject one service. Submit a method reference."
  tagline: Persistent, retrying background jobs on plain Jakarta EE. No proprietary dependencies, no message broker, and they run unchanged on whichever application server you already use.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started/introduction
    - theme: alt
      text: API Reference
      link: /api-reference/overview

features:
  - title: CDI-native API
    details: Inject JobSchedulerService like any other bean and hand it a method reference. Ratchet persists it, runs it, retries on failure, and fires events you can observe.
  - title: Resilience built in
    details: Retries with fixed or exponential backoff, a circuit breaker for flaky downstreams, a dead-letter queue, and pause, resume, and cancel at runtime. It's part of the scheduler contract, not a library you bolt on.
  - title: Override any default
    details: "Because it's built on CDI, every default is a bean you can replace. Need to encrypt payloads with your behind-the-firewall KMS? Implement the key provider. Want a different cluster coordinator, retry policy, or store? Swap it in with a standard @Alternative. It's plain CDI, not a plugin system you have to learn."
---
