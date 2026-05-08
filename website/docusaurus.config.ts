import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const umamiHost = process.env.UMAMI_HOST;
const umamiSiteId = process.env.UMAMI_SITE_ID;
const umamiEnabled =
  process.env.NODE_ENV === 'production' && !!umamiHost && !!umamiSiteId;

const config: Config = {
  title: 'Ratchet',
  tagline: 'The background job scheduler Jakarta EE has been missing.',
  favicon: 'img/favicon.ico',

  headTags: [
    {
      tagName: 'link',
      attributes: { rel: 'icon', type: 'image/svg+xml', href: '/img/favicon.svg' },
    },
    {
      tagName: 'link',
      attributes: { rel: 'apple-touch-icon', sizes: '180x180', href: '/img/apple-touch-icon.png' },
    },
    {
      tagName: 'link',
      attributes: { rel: 'manifest', href: '/site.webmanifest' },
    },
    ...(umamiEnabled
      ? [
          {
            tagName: 'script',
            attributes: {
              src: `${umamiHost}/script.js`,
              'data-website-id': umamiSiteId!,
              defer: 'true',
            },
          },
        ]
      : []),
  ],

  future: {
    v4: true,
  },

  url: 'https://ratchet.run',
  baseUrl: '/',

  organizationName: 'ratchet-run',
  projectName: 'ratchet',

  onBrokenLinks: 'warn',

  clientModules: [require.resolve('./src/clientModules/umamiTracking.ts')],

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
            'https://github.com/ratchet-run/ratchet/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/logo.svg',
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
          sidebarId: 'deploymentSidebar',
          position: 'left',
          label: 'Deployment',
        },
        {
          type: 'docSidebar',
          sidebarId: 'conformanceSidebar',
          position: 'left',
          label: 'Conformance',
        },
        {
          href: 'https://github.com/ratchet-run/ratchet',
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
            {
              label: 'Conformance',
              to: '/docs/conformance',
            },
            {
              label: 'Advanced',
              to: '/docs/advanced/customization-platform',
            },
            {
              label: 'Troubleshooting',
              to: '/docs/troubleshooting/overview',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/ratchet-run/ratchet',
            },
            {
              label: 'Issues',
              href: 'https://github.com/ratchet-run/ratchet/issues',
            },
            {
              label: 'Discussions',
              href: 'https://github.com/ratchet-run/ratchet/discussions',
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
