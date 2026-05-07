import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import Heading from '@theme/Heading';

import styles from './index.module.css';

const quickStart = `@Inject
JobSchedulerService scheduler;

public void placeOrder(UUID orderId) {
    scheduler.enqueue(() -> orders.process(orderId))
        .withMaxRetries(3)
        .withBackoff(
            BackoffPolicy.EXPONENTIAL,
            Duration.ofSeconds(2))
        .withTags("orders", "fulfillment")
        .submit();
}`;

const highlights = [
  {
    title: 'CDI-first scheduling',
    description:
      'Inject one service, submit serializable method calls, and let Ratchet persist, claim, execute, retry, and observe the work.',
  },
  {
    title: 'Stores you can prove',
    description:
      'MySQL, PostgreSQL, and MongoDB stores ship with the reference implementation and are covered by a reusable store TCK.',
  },
  {
    title: 'Operational primitives',
    description:
      'Retries, backoff, circuit breakers, job control, batch progress, signal waiting, and query APIs are part of the scheduler contract.',
  },
];

const docs = [
  {
    title: 'Install Ratchet',
    href: '/docs/getting-started/installation',
    description: 'Import the BOM, choose a store, apply schema, and produce runtime options.',
  },
  {
    title: 'Run a first job',
    href: '/docs/getting-started/quickstart',
    description: 'A short path from CDI injection to an executed persisted job.',
  },
  {
    title: 'Check the API',
    href: '/docs/api-reference/overview',
    description: 'Current public API, SPI, events, query service, and builder references.',
  },
  {
    title: 'Review conformance',
    href: '/docs/conformance',
    description: 'Store, API, and Jakarta runtime compatibility reports.',
  },
];

function Highlight({title, description}: {title: string; description: string}) {
  return (
    <article className={styles.highlight}>
      <Heading as="h3">{title}</Heading>
      <p>{description}</p>
    </article>
  );
}

function DocLink({title, href, description}: {title: string; href: string; description: string}) {
  return (
    <Link className={styles.docLink} to={href}>
      <span>{title}</span>
      <small>{description}</small>
    </Link>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="Ratchet"
      description="Portable, CDI-based job scheduler for Jakarta EE 10/11 applications">
      <main>
        <section className={styles.hero}>
          <div className={styles.heroInner}>
            <img className={styles.heroLogo} src="/img/logo.svg" alt="" aria-hidden="true" />
            <p className={styles.eyebrow}>Jakarta EE background jobs</p>
            <Heading as="h1">Ratchet</Heading>
            <p className={styles.subtitle}>
              A CDI-native scheduler for persistent jobs, retries, batches, workflows, signals, and
              operational dashboards.
            </p>
            <div className={styles.actions}>
              <Link
                className={`button button--primary button--lg ${styles.primaryCta}`}
                to="/docs/getting-started/introduction">
                Get Started
              </Link>
              <Link
                className={`button button--outline button--secondary button--lg ${styles.secondaryCta}`}
                to="/docs/api-reference/overview">
                API Reference
              </Link>
            </div>
            <div className={styles.codePreview}>
              <CodeBlock language="java">{quickStart}</CodeBlock>
            </div>
          </div>
        </section>

        <section className={styles.band}>
          <div className={styles.grid}>
            {highlights.map((item) => (
              <Highlight key={item.title} {...item} />
            ))}
          </div>
        </section>

        <section className={styles.docsBand}>
          <div className={styles.docsInner}>
            <div>
              <p className={styles.eyebrow}>Documentation paths</p>
              <Heading as="h2">Start from the job you need to do.</Heading>
            </div>
            <div className={styles.docGrid}>
              {docs.map((item) => (
                <DocLink key={item.title} {...item} />
              ))}
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
