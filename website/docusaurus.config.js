import {themes as prismThemes} from 'prism-react-renderer';

const config = {
  title: 'Chronos',
  tagline: 'Branch data across databases, filesystems, and object stores',
  favicon: 'icons/chronos-icon.svg',

  url: 'https://mitdbg.github.io',
  baseUrl: '/chronos/',
  organizationName: 'mitdbg',
  projectName: 'chronos',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  staticDirectories: ['static', '../assets'],

  presets: [
    [
      'classic',
      {
        docs: {
          path: '../docs',
          routeBasePath: 'docs',
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/mitdbg/chronos/edit/main/docs/',
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      },
    ],
  ],

  themeConfig: {
    metadata: [
      {
        name: 'description',
        content:
          'Chronos creates writable branches across databases, filesystems, and object stores without copying the full dataset.',
      },
    ],
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Chronos',
      logo: {
        alt: 'Chronos',
        src: 'icons/chronos-icon.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/tutorials/software-development',
          label: 'Tutorials',
          position: 'left',
        },
        {
          href: 'https://github.com/mitdbg/postgres_chronos',
          label: 'Chronos for PostgreSQL',
          position: 'left',
        },
        {
          href: 'https://github.com/mitdbg/chronos',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Learn',
          items: [
            {label: 'Get started', to: '/docs/installation'},
            {label: 'Integration guide', to: '/docs/integration'},
            {label: 'Compatibility', to: '/docs/compatibility'},
          ],
        },
        {
          title: 'Use cases',
          items: [
            {
              label: 'Software development',
              to: '/docs/tutorials/software-development',
            },
            {
              label: 'RL data sandboxes',
              to: '/docs/tutorials/rl-data-sandbox',
            },
            {label: 'Multi-store branching', to: '/docs/multi-store-branching'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/mitdbg/chronos'},
            {
              label: 'PostgreSQL implementation',
              href: 'https://github.com/mitdbg/postgres_chronos',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} MIT Database Group.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'sql', 'toml'],
    },
  },
};

export default config;
