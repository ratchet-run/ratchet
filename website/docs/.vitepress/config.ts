import { defineConfig, type HeadConfig } from 'vitepress'
import { heroCodePlugin } from './heroCodePlugin'

const umamiHost = process.env.UMAMI_HOST
const umamiSiteId = process.env.UMAMI_SITE_ID
const umamiEnabled =
  process.env.NODE_ENV === 'production' && !!umamiHost && !!umamiSiteId

const umamiHead: HeadConfig[] = umamiEnabled
  ? [
      [
        'script',
        {
          src: `${umamiHost}/script.js`,
          'data-website-id': umamiSiteId!,
          defer: 'true',
        },
      ],
      [
        'script',
        {
          src: `${umamiHost}/recorder.js`,
          'data-website-id': umamiSiteId!,
          'data-sample-rate': '1',
          'data-mask-level': 'moderate',
          defer: 'true',
        },
      ],
    ]
  : []

export default defineConfig({
  title: 'Ratchet',
  description: 'The background job scheduler Jakarta EE has been missing.',

  head: [
    ['link', { rel: 'icon', href: '/img/favicon.ico' }],
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/img/favicon.svg' }],
    ['link', { rel: 'apple-touch-icon', sizes: '180x180', href: '/img/apple-touch-icon.png' }],
    ['link', { rel: 'manifest', href: '/site.webmanifest' }],
    ...umamiHead,
  ],

  themeConfig: {
    logo: '/img/logo.svg',
    siteTitle: 'Ratchet',

    nav: [
      { text: 'Getting Started', link: '/getting-started/introduction' },
      { text: 'Compare', link: '/comparison/overview' },
      { text: 'Concepts', link: '/concepts/overview' },
      { text: 'API Reference', link: '/api-reference/overview' },
      { text: 'Deployment', link: '/deployment/overview' },
      { text: 'Conformance', link: '/conformance/' },
    ],

    sidebar: {
      '/getting-started/': [
        {
          text: 'Getting Started',
          items: [
            { text: 'Introduction', link: '/getting-started/introduction' },
            { text: 'Installation', link: '/getting-started/installation' },
            { text: 'Quickstart', link: '/getting-started/quickstart' },
            { text: 'Basic Concepts', link: '/getting-started/basic-concepts' },
            { text: 'First Job', link: '/getting-started/first-job' },
            { text: 'Configuration', link: '/getting-started/configuration' },
          ],
        },
      ],
      '/concepts/': [
        {
          text: 'Concepts',
          items: [
            { text: 'Overview', link: '/concepts/overview' },
            { text: 'Job Lifecycle', link: '/concepts/job-lifecycle' },
            { text: 'Job Types', link: '/concepts/job-types' },
            { text: 'Scheduling', link: '/concepts/scheduling' },
            { text: 'Execution Model', link: '/concepts/execution-model' },
            { text: 'Error Handling', link: '/concepts/error-handling' },
            { text: 'Retry Strategies', link: '/concepts/retry-strategies' },
            { text: 'Batches', link: '/concepts/batches' },
            { text: 'Workflows', link: '/concepts/workflows' },
            { text: 'Persistence', link: '/concepts/persistence' },
            { text: 'Clustering', link: '/concepts/clustering' },
          ],
        },
      ],
      '/api-reference/': [
        {
          text: 'API Reference',
          items: [
            { text: 'Overview', link: '/api-reference/overview' },
            { text: 'JobSchedulerService', link: '/api-reference/job-scheduler-service' },
            { text: 'JobQueryService', link: '/api-reference/job-query-service' },
            { text: 'JobBuilder', link: '/api-reference/job-builder' },
            { text: 'JobOptions', link: '/api-reference/job-options' },
            { text: 'BatchBuilder', link: '/api-reference/batch-builder' },
            { text: 'BatchContext', link: '/api-reference/batch-context' },
            { text: 'JobContext', link: '/api-reference/job-context' },
            { text: 'JobResult', link: '/api-reference/job-result' },
            { text: 'Annotations', link: '/api-reference/annotations' },
            { text: 'Event System', link: '/api-reference/event-system' },
            { text: 'Functional Interfaces', link: '/api-reference/functional-interfaces' },
            { text: 'Workflow Condition', link: '/api-reference/workflow-condition' },
            { text: 'SPI Interfaces', link: '/api-reference/spi-interfaces' },
          ],
        },
      ],
      '/advanced/': [
        {
          text: 'Advanced',
          items: [
            { text: 'Customization Platform', link: '/advanced/customization-platform' },
            { text: 'SPI Implementation', link: '/advanced/spi-implementation' },
            { text: 'Circuit Breakers', link: '/advanced/circuit-breakers' },
            { text: 'Custom Retry Policies', link: '/advanced/custom-retry-policies' },
            { text: 'Custom Serialization', link: '/advanced/custom-serialization' },
            { text: 'Custom Logging', link: '/advanced/custom-logging' },
            { text: 'Metrics Collection', link: '/advanced/metrics-collection' },
          ],
        },
      ],
      '/deployment/': [
        {
          text: 'Deployment',
          items: [
            { text: 'Overview', link: '/deployment/overview' },
            { text: 'Installation', link: '/deployment/installation' },
            { text: 'Configuration', link: '/deployment/configuration' },
            { text: 'Database Setup', link: '/deployment/database-setup' },
            { text: 'MySQL', link: '/deployment/mysql' },
            { text: 'PostgreSQL', link: '/deployment/postgresql' },
            { text: 'MongoDB', link: '/deployment/mongodb' },
            { text: 'Clustering', link: '/deployment/clustering' },
            { text: 'Cluster Coordinators', link: '/deployment/cluster-coordinators' },
            { text: 'Docker', link: '/deployment/docker' },
            { text: 'Kubernetes', link: '/deployment/kubernetes' },
            { text: 'Cluster Configuration', link: '/deployment/cluster-configuration' },
            { text: 'Performance Tuning', link: '/deployment/performance-tuning' },
            { text: 'Monitoring', link: '/deployment/monitoring' },
            { text: 'Troubleshooting', link: '/deployment/troubleshooting' },
          ],
        },
      ],
      '/troubleshooting/': [
        {
          text: 'Troubleshooting',
          items: [
            { text: 'Overview', link: '/troubleshooting/overview' },
            { text: 'Common Issues', link: '/troubleshooting/common-issues' },
            { text: 'Debugging', link: '/troubleshooting/debugging' },
            { text: 'FAQ', link: '/troubleshooting/faq' },
          ],
        },
      ],
      '/comparison/': [
        {
          text: 'Compare',
          items: [
            { text: 'Overview', link: '/comparison/overview' },
            { text: 'vs Quartz', link: '/comparison/vs-quartz' },
            { text: 'vs JobRunr', link: '/comparison/vs-jobrunr' },
            { text: 'vs Spring Batch', link: '/comparison/vs-spring-batch' },
            { text: 'vs jBeret', link: '/comparison/vs-jberet' },
            { text: 'vs db-scheduler', link: '/comparison/vs-db-scheduler' },
          ],
        },
      ],
      '/conformance/': [
        {
          text: 'Conformance',
          items: [
            { text: 'Overview', link: '/conformance/' },
            {
              text: 'Store Reports',
              collapsed: false,
              items: [
                { text: 'MySQL', link: '/conformance/mysql' },
                { text: 'PostgreSQL', link: '/conformance/postgresql' },
                { text: 'MongoDB', link: '/conformance/mongodb' },
              ],
            },
            {
              text: 'API Reports',
              link: '/conformance/api/',
              collapsed: true,
              items: [
                { text: 'WildFly + MySQL', link: '/conformance/api/wildfly-managed-mysql' },
                { text: 'WildFly + PostgreSQL', link: '/conformance/api/wildfly-managed-postgresql' },
                { text: 'WildFly + MongoDB', link: '/conformance/api/wildfly-managed-mongodb' },
                { text: 'WildFly EE 11 + MySQL', link: '/conformance/api/wildfly-ee11-managed-mysql' },
                { text: 'WildFly EE 11 + PostgreSQL', link: '/conformance/api/wildfly-ee11-managed-postgresql' },
                { text: 'WildFly EE 11 + MongoDB', link: '/conformance/api/wildfly-ee11-managed-mongodb' },
                { text: 'Payara + MySQL', link: '/conformance/api/payara-managed-mysql' },
                { text: 'Payara + PostgreSQL', link: '/conformance/api/payara-managed-postgresql' },
                { text: 'Payara + MongoDB', link: '/conformance/api/payara-managed-mongodb' },
                { text: 'OpenLiberty + MySQL', link: '/conformance/api/openliberty-managed-mysql' },
                { text: 'OpenLiberty + PostgreSQL', link: '/conformance/api/openliberty-managed-postgresql' },
                { text: 'OpenLiberty + MongoDB', link: '/conformance/api/openliberty-managed-mongodb' },
                { text: 'GlassFish + MySQL', link: '/conformance/api/glassfish-managed-mysql' },
                { text: 'GlassFish + PostgreSQL', link: '/conformance/api/glassfish-managed-postgresql' },
                { text: 'GlassFish + MongoDB', link: '/conformance/api/glassfish-managed-mongodb' },
              ],
            },
            {
              text: 'Jakarta Runtime Reports',
              link: '/conformance/jakarta/',
              collapsed: true,
              items: [
                { text: 'WildFly + MySQL', link: '/conformance/jakarta/wildfly-managed-mysql' },
                { text: 'WildFly + PostgreSQL', link: '/conformance/jakarta/wildfly-managed-postgresql' },
                { text: 'WildFly + MongoDB', link: '/conformance/jakarta/wildfly-managed-mongodb' },
                { text: 'WildFly EE 11 + MySQL', link: '/conformance/jakarta/wildfly-ee11-managed-mysql' },
                { text: 'WildFly EE 11 + PostgreSQL', link: '/conformance/jakarta/wildfly-ee11-managed-postgresql' },
                { text: 'WildFly EE 11 + MongoDB', link: '/conformance/jakarta/wildfly-ee11-managed-mongodb' },
                { text: 'Payara + MySQL', link: '/conformance/jakarta/payara-managed-mysql' },
                { text: 'Payara + PostgreSQL', link: '/conformance/jakarta/payara-managed-postgresql' },
                { text: 'Payara + MongoDB', link: '/conformance/jakarta/payara-managed-mongodb' },
                { text: 'OpenLiberty + MySQL', link: '/conformance/jakarta/openliberty-managed-mysql' },
                { text: 'OpenLiberty + PostgreSQL', link: '/conformance/jakarta/openliberty-managed-postgresql' },
                { text: 'OpenLiberty + MongoDB', link: '/conformance/jakarta/openliberty-managed-mongodb' },
                { text: 'GlassFish + MySQL', link: '/conformance/jakarta/glassfish-managed-mysql' },
                { text: 'GlassFish + PostgreSQL', link: '/conformance/jakarta/glassfish-managed-postgresql' },
                { text: 'GlassFish + MongoDB', link: '/conformance/jakarta/glassfish-managed-mongodb' },
              ],
            },
          ],
        },
      ],
      '/adr/': [
        {
          text: 'Architecture Decision Records',
          items: [
            { text: 'Overview', link: '/adr/' },
            {
              text: '0001 — Payload encryption threat model',
              link: '/adr/0001-payload-encryption-threat-model',
            },
          ],
        },
      ],
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/ratchet-run/ratchet' },
    ],

    search: {
      provider: 'local',
    },

    editLink: {
      pattern: 'https://github.com/ratchet-run/ratchet/tree/main/website/docs/:path',
      text: 'Edit this page on GitHub',
    },
  },

  markdown: {
    theme: {
      light: 'github-light',
      dark: 'dracula',
    },
  },

  vite: {
    plugins: [heroCodePlugin()],
  },
})
