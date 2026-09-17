import React from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import styles from './index.module.css';

const useCases = [
  {
    title: 'Agent data sandboxes',
    description:
      'Give each agent run or RL rollout private data state to change and evaluate.',
    link: '/docs/tutorials/rl-data-sandbox',
    linkLabel: 'verl and E2B tutorial',
  },
  {
    title: 'Development and testing',
    description:
      'Try fixes or migrations against realistic data without changing the shared source.',
    link: '/docs/tutorials/software-development',
    linkLabel: 'Software development tutorial',
  },
  {
    title: 'What-if exploration',
    description:
      'Compare alternative plans or data changes from the same starting point, then keep the result you want.',
    link: '/docs/branching-introduction',
    linkLabel: 'Branching guide',
  },
];

const capabilities = [
  {
    title: 'Fast branching',
    description: 'Create writable branches in milliseconds, independent of the source database’s size.',
  },
  {
    title: 'Copy-on-write',
    description: 'Branches share unchanged data with their source.',
  },
  {
    title: 'Branch isolation',
    description: 'Each branch sees its own changes without affecting its parent or sibling branches.',
  },
  {
    title: 'Efficient query',
    description: 'Query performance stays stable as the number of branches grows.',
  },
];

const stores = ['PostgreSQL', 'SQLite', 'DuckDB', 'Qdrant', 'ChronosFS', 'S3-compatible'];

function StoreIcon({kind}) {
  const shapes = {
    relational: <><rect x="3" y="4" width="18" height="16" rx="2" /><path d="M3 9h18M9 9v11M15 9v11" /></>,
    nosql: <><circle cx="5" cy="6" r="2" /><circle cx="19" cy="6" r="2" /><circle cx="12" cy="18" r="2" /><path d="M7 7.5l4 8.5M17 7.5l-4 8.5M7 6h10" /></>,
    filesystem: <path d="M2.5 7a2 2 0 0 1 2-2h5l2 2h8a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-15a2 2 0 0 1-2-2z" />,
  };

  return <svg className={styles.storeIcon} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{shapes[kind]}</svg>;
}

function BranchGraphic() {
  return (
    <div className={styles.branchGraphic} role="img" aria-label="A shared relational database, vector or NoSQL database, and filesystem branch into separate agent, test, and what-if sandboxes">
      <div className={styles.graphTitle}>Shared application state</div>
      <div className={styles.dataSystems}>
        <div><StoreIcon kind="relational" /><span>Relational DB</span></div>
        <div><StoreIcon kind="nosql" /><span>Vector / NoSQL</span></div>
        <div><StoreIcon kind="filesystem" /><span>Filesystem</span></div>
      </div>
      <div className={styles.branchConnector} aria-hidden="true">
        <svg className={styles.branchLines} viewBox="0 0 600 78" preserveAspectRatio="none">
          <path d="M300 2v25 M100 27h400 M100 27v39 M300 27v39 M500 27v39" />
          <path d="M93 58l7 9 7-9 M293 58l7 9 7-9 M493 58l7 9 7-9" />
        </svg>
        <span className={styles.forkLabel}>fork</span>
      </div>
      <div className={styles.branchExamples}>
        {['Agent run', 'Test', 'What-if'].map((name) => (
          <div key={name}>
            <strong>{name}</strong>
            <span className={styles.branchStoreMarks} aria-hidden="true"><i /><i /><i /></span>
          </div>
        ))}
      </div>
      <p className={styles.graphicCaption}>Shared starting point. Private writes in every branch.</p>
    </div>
  );
}

function Home() {
  return (
    <Layout
      title="Lightweight Data Sandbox"
      description="Chronos gives agents and applications isolated, writable data sandboxes across relational databases, NoSQL databases, and filesystems without copying entire stores.">
      <main>
        <header className={styles.hero}>
          <div className={`container ${styles.heroGrid}`}>
            <div className={styles.heroCopy}>
              <Heading as="h1">Lightweight Data Sandbox</Heading>
              <p className={styles.heroLead}>
                Chronos gives each agent, RL rollout, or what-if experiment an isolated, writable branch
                of application state across relational databases, vector and other NoSQL databases,
                and filesystems. Branches share unchanged data instead of copying entire stores.
              </p>
              <div className={styles.heroActions}>
                <Link className={styles.primaryButton} to="/docs/installation">
                  Get started <span aria-hidden="true">→</span>
                </Link>
                <Link className={styles.secondaryButton} to="/docs/branching-introduction">
                  How branching works
                </Link>
              </div>
              <p className={styles.statusLine}>
                Experimental release. <Link to="/docs/compatibility">Check compatibility and limits.</Link>
              </p>
            </div>
            <BranchGraphic />
          </div>
        </header>

        <section className={styles.storeStrip} aria-label="Supported data stores">
          <div className="container">
            <p>Implemented for</p>
            <div className={styles.storeList}>
              {stores.map((store) => <span key={store}>{store}</span>)}
            </div>
          </div>
        </section>

        <section className={styles.section}>
          <div className="container">
            <div className={styles.sectionHeading}>
              <Heading as="h2">What Chronos gives you</Heading>
            </div>
            <div className={styles.capabilityGrid}>
              {capabilities.map((item) => (
                <article className={styles.capability} key={item.title}>
                  <Heading as="h3">{item.title}</Heading>
                  <p>{item.description}</p>
                </article>
              ))}
            </div>
            <p className={styles.runtimeNote}>
              A runtime sandbox isolates code; Chronos gives it private data.{' '}
              <Link to="/docs/tutorials/rl-data-sandbox">See the verl and E2B example.</Link>
            </p>
          </div>
        </section>

        <section className={`${styles.section} ${styles.useCasesSection}`}>
          <div className="container">
            <div className={styles.sectionHeading}>
              <Heading as="h2">Where people use it</Heading>
            </div>
            <div className={styles.useCaseGrid}>
              {useCases.map((item) => (
                <article className={styles.useCaseCard} key={item.title}>
                  <Heading as="h3">{item.title}</Heading>
                  <p>{item.description}</p>
                  <Link to={item.link}>{item.linkLabel} <span aria-hidden="true">→</span></Link>
                </article>
              ))}
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Home;
