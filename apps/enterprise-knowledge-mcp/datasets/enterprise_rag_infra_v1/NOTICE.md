# Dataset attribution

The company records originate from
[EnterpriseRAG-Bench](https://github.com/onyx-dot-app/EnterpriseRAG-Bench), commit
`d36685e273713975ee20299bbf1ab64165575b3c`, under its MIT license (copyright
2026 DanswerAI, Inc.; full text in `ENTERPRISERAG-LICENSE`). Redwood Inference
is synthetic, not a release of a real company's private Slack/email records.

The source archive includes pinned vLLM, LiteLLM and Langfuse source trees.
Their original license/notice files remain with them; the dataset does not
relicense upstream code. Git internals and separately licensed Langfuse
enterprise directories are excluded. See `seed/codebases/manifest.json` for
revisions and indexing policy.

Public GitHub issue/PR/release text and API provenance retain their source
URLs and authorship metadata. They are included for research provenance;
Chronos's MIT license does not imply ownership or blanket relicensing of
third-party contributions. The prepared SQLite snapshot is a derived
representation of the same inputs. Authored synthetic augmentation records
and preparation code are provided with this repository.
