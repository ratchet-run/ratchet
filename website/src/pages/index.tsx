import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/getting-started/introduction">
            Get Started
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/api-reference/overview"
            style={{marginLeft: '1rem'}}>
            API Reference
          </Link>
        </div>
      </div>
    </header>
  );
}

const features = [
  {
    title: 'CDI-Native',
    description: 'Inject JobSchedulerService into any CDI bean. Zero ceremony — declare @Recurring methods and they\'re automatically discovered at startup.',
  },
  {
    title: 'Resilient by Default',
    description: 'Built-in circuit breaker, configurable retry with exponential backoff, dead letter queue, and timeout watchdog. No external resilience library needed.',
  },
  {
    title: 'Workflow Orchestration',
    description: 'Chain jobs with conditional branching, build parallel batches, stream large datasets — all with a fluent type-safe API.',
  },
  {
    title: 'Pluggable Storage',
    description: 'MySQL, PostgreSQL, and MongoDB out of the box. Implement the store SPI for any backend and validate with the TCK.',
  },
  {
    title: 'Observable',
    description: 'Rich event system via CDI @Observes or programmatic listeners. Optional Micrometer adapter for metrics dashboards.',
  },
  {
    title: 'Enterprise Ready',
    description: 'Jakarta EE 10, Java 17+, container-managed transactions, safe multi-node job claiming, database-backed singleton recurring scans, and optional cross-node wakeups.',
  },
];

function Feature({title, description}: {title: string; description: string}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md padding-vert--lg">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Home"
      description="Portable, CDI-based job scheduler for Jakarta EE 10 applications">
      <HomepageHeader />
      <main>
        <section className="container margin-vert--xl">
          <div className="row">
            {features.map((props, idx) => (
              <Feature key={idx} {...props} />
            ))}
          </div>
        </section>
      </main>
    </Layout>
  );
}
