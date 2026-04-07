import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Ratchet',
  tagline: 'Portable CDI-based Job Scheduler for Jakarta EE',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://ratchet.jcputney.dev',
  baseUrl: '/',

  organizationName: 'jcputney',
  projectName: 'ratchet',

  onBrokenLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/jcputney/ratchet/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/ratchet-social-card.jpg',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Ratchet',
      logo: {
        alt: 'Ratchet Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'gettingStartedSidebar',
          position: 'left',
          label: 'Getting Started',
        },
        {
          type: 'docSidebar',
          sidebarId: 'conceptsSidebar',
          position: 'left',
          label: 'Concepts',
        },
        {
          type: 'docSidebar',
          sidebarId: 'apiReferenceSidebar',
          position: 'left',
          label: 'API Reference',
        },
        {
          type: 'docSidebar',
          sidebarId: 'advancedSidebar',
          position: 'left',
          label: 'Advanced',
        },
        {
          type: 'docSidebar',
          sidebarId: 'deploymentSidebar',
          position: 'left',
          label: 'Deployment',
        },
        {
          type: 'docSidebar',
          sidebarId: 'troubleshootingSidebar',
          position: 'left',
          label: 'Troubleshooting',
        },
        {
          href: 'https://github.com/jcputney/ratchet',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/getting-started/introduction',
            },
            {
              label: 'API Reference',
              to: '/docs/api-reference/overview',
            },
            {
              label: 'Deployment',
              to: '/docs/deployment/overview',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/jcputney/ratchet',
            },
            {
              label: 'Issues',
              href: 'https://github.com/jcputney/ratchet/issues',
            },
            {
              label: 'Discussions',
              href: 'https://github.com/jcputney/ratchet/discussions',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Ratchet Project. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['java', 'bash', 'sql', 'json', 'markup', 'yaml', 'properties'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
