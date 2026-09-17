import React from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './index.module.css';

const useCases = [
  {
    number: '01',
    title: 'Develop against real state',
    description:
      'Give every fix or migration an isolated branch of its code and data, then inspect and publish the accepted result.',
    link: '/docs/tutorials/software-development',
    linkLabel: 'Software development tutorial',
  },
  {
    number: '02',
    title: 'Run parallel agent rollouts',
    description:
      'Start each rollout from shared state, let tools read and write freely, compute the reward, and discard the branch.',
    link: '/docs/tutorials/rl-data-sandbox',
    linkLabel: 'RL sandbox tutorial',
  },
  {
    number: '03',
    title: 'Coordinate several stores',
    description:
      'Use one branch identity across relational data, files, vectors, and objects instead of building per-store lifecycle logic.',
    link: '/docs/multi-store-branching',
    linkLabel: 'Multi-store guide',
  },
];

const stores = ['PostgreSQL', 'SQLite', 'DuckDB', 'Qdrant', 'ChronosFS', 'S3-compatible'];

function BranchGraphic() {
  return (
    <div className={styles.branchGraphic} aria-label="One shared state branching into isolated workspaces">
      <div className={`${styles.dataNode} ${styles.rootNode}`}>
        <span>shared state</span>
        <strong>main</strong>
      </div>
      <div className={styles.branchLine} aria-hidden="true" />
      <div className={styles.childNodes}>
        <div className={`${styles.dataNode} ${styles.cyanNode}`}>
          <span>branch</span>
          <strong>trial-a</strong>
        </div>
        <div className={`${styles.dataNode} ${styles.violetNode}`}>
          <span>branch</span>
          <strong>trial-b</strong>
        </div>
      </div>
      <div className={styles.graphicCaption}>isolate · inspect · merge or discard</div>
    </div>
  );
}

function Home() {
  const iconUrl = useBaseUrl('/icons/chronos-icon.svg');

  return (
    <Layout
      title="Branch data without copying it"
      description="Chronos creates writable branches across databases, filesystems, and object stores.">
      <main>
        <header className={styles.hero}>
          <div className={styles.heroBackdrop} aria-hidden="true" />
          <div className={`container ${styles.heroGrid}`}>
            <div className={styles.heroCopy}>
              <div className={styles.eyebrow}>
                <img src={iconUrl} alt="" />
                Branching for stateful applications
              </div>
              <Heading as="h1">Branch data.<br />Keep momentum.</Heading>
              <p className={styles.heroLead}>
                Chronos creates writable branches across databases, filesystems, and object
                stores—without copying the full dataset or rebuilding your application around a
                new storage system.
              </p>
              <div className={styles.heroActions}>
                <Link className={styles.primaryButton} to="/docs/installation">
                  Get started <span aria-hidden="true">→</span>
                </Link>
                <Link className={styles.secondaryButton} to="/docs/branching-introduction">
                  How it works
                </Link>
              </div>
              <div className={styles.statusLine}>
                <span className={styles.statusDot} /> Experimental open-source release
              </div>
            </div>
            <BranchGraphic />
          </div>
        </header>

        <section className={styles.storeStrip} aria-label="Supported data stores">
          <div className="container">
            <p>One branch abstraction across your existing data</p>
            <div className={styles.storeList}>
              {stores.map((store) => <span key={store}>{store}</span>)}
            </div>
          </div>
        </section>

        <section className={styles.section}>
          <div className="container">
            <div className={styles.sectionHeading}>
              <span>Built for speculative work</span>
              <Heading as="h2">Try the change without moving the data.</Heading>
              <p>
                Branches share unchanged state. Each branch sees its own writes while its parent
                and sibling branches remain isolated.
              </p>
            </div>
            <div className={styles.useCaseGrid}>
              {useCases.map((item) => (
                <article className={styles.useCaseCard} key={item.number}>
                  <span className={styles.cardNumber}>{item.number}</span>
                  <Heading as="h3">{item.title}</Heading>
                  <p>{item.description}</p>
                  <Link to={item.link}>{item.linkLabel} <span aria-hidden="true">→</span></Link>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section className={`${styles.section} ${styles.codeSection}`}>
          <div className={`container ${styles.codeGrid}`}>
            <div>
              <span className={styles.sectionLabel}>A small application surface</span>
              <Heading as="h2">Branch around the work you already do.</Heading>
              <p>
                Register the data Chronos should manage, create a branch, and run ordinary reads
                and writes inside its context. Keep the branch for review, merge it, or delete it.
              </p>
              <Link className={styles.textLink} to="/docs/integration">
                Read the integration guide <span aria-hidden="true">→</span>
              </Link>
            </div>
            <div className={styles.codeWindow}>
              <div className={styles.codeHeader}>
                <span /><span /><span />
                <small>branch.py</small>
              </div>
              <pre><code>{`ctx.create_branch("trial", from_branch="main")

with ctx.checkout("trial") as branch:
    branch.execute(
        "UPDATE items SET quantity = :n",
        {"n": 7},
    )

ctx.merge_apply("trial", "main")
ctx.delete_branch("trial")`}</code></pre>
            </div>
          </div>
        </section>

        <section className={`${styles.section} ${styles.pathsSection}`}>
          <div className="container">
            <div className={styles.sectionHeading}>
              <span>Choose the integration that fits</span>
              <Heading as="h2">Across stores, or directly inside PostgreSQL.</Heading>
            </div>
            <div className={styles.pathGrid}>
              <article>
                <div className={styles.pathTag}>This repository</div>
                <Heading as="h3">Bolt-on Chronos</Heading>
                <p>
                  Add branching to an application that spans several existing databases,
                  filesystems, or object stores through one Python library.
                </p>
                <Link to="/docs/installation">Install the library <span aria-hidden="true">→</span></Link>
              </article>
              <article>
                <div className={styles.pathTag}>PostgreSQL source tree</div>
                <Heading as="h3">Chronos for PostgreSQL</Heading>
                <p>
                  Use interval-based branching implemented inside PostgreSQL when all of your
                  branchable state lives in a single database.
                </p>
                <a href="https://github.com/mitdbg/postgres_chronos">
                  Visit the PostgreSQL project <span aria-hidden="true">↗</span>
                </a>
              </article>
            </div>
          </div>
        </section>

        <section className={styles.ctaSection}>
          <div className="container">
            <div className={styles.ctaCard}>
              <img src={iconUrl} alt="" />
              <div>
                <Heading as="h2">Create your first branch.</Heading>
                <p>Start with SQLite in memory, then connect the stores your application uses.</p>
              </div>
              <Link className={styles.primaryButton} to="/docs/installation">
                Install Chronos <span aria-hidden="true">→</span>
              </Link>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Home;
