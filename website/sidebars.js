const sidebars = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Start here',
      collapsed: false,
      items: [
        {type: 'doc', id: 'README', label: 'Overview'},
        {type: 'doc', id: 'installation', label: 'Installation'},
        {type: 'doc', id: 'integration', label: 'Integrate Chronos'},
        {type: 'doc', id: 'compatibility', label: 'Compatibility and limits'},
      ],
    },
    {
      type: 'category',
      label: 'Tutorials',
      items: [
        {
          type: 'doc',
          id: 'tutorials/software-development',
          label: 'Software development',
        },
        {
          type: 'doc',
          id: 'tutorials/rl-data-sandbox',
          label: 'RL data sandboxes',
        },
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        {type: 'doc', id: 'multi-store-branching', label: 'Multi-store branching'},
        {type: 'doc', id: 'filesystem-on-chronos', label: 'ChronosFS'},
        {type: 'doc', id: 'mcp-integration', label: 'MCP integration'},
      ],
    },
    {
      type: 'category',
      label: 'Concepts and internals',
      items: [
        {type: 'doc', id: 'branching-introduction', label: 'Branching model'},
        {type: 'doc', id: 'branching-transaction', label: 'Branch transactions'},
        {type: 'doc', id: 'bolt-on-branching', label: 'Interval versioning'},
        {
          type: 'doc',
          id: 'multi-store-branching-applications',
          label: 'Applications',
        },
        {
          type: 'doc',
          id: 'multi-store-branching-abstract',
          label: 'Research abstract',
        },
        {type: 'doc', id: 'related-work', label: 'Related work'},
      ],
    },
  ],
};

export default sidebars;
