# Chronos website

The website uses Docusaurus and renders the maintained Markdown files in the
repository-level `docs/` directory. Do not copy documentation into this folder.

Install dependencies and start the development server:

```sh
cd website
npm install
npm start
```

Create the production site with:

```sh
npm run build
```

The default deployment configuration targets
`https://mitdbg.github.io/chronos/`. Run the generated site locally with
`npm run serve`.
