"""Initialize a fresh tutorial database and optionally write verl prompt parquet."""

import argparse
from pathlib import Path

from chronos_core.branching import ChronosBranchContext

PROMPT = "Inspect the support tickets. Set the payment outage ticket (101) to high priority. Leave every other ticket unchanged."


def initialize(url, metadata_url=None):
    if metadata_url:
        ctx = ChronosBranchContext.connect_split(
            url,
            metadata_url,
            backend="interval",
        )
    else:
        ctx = ChronosBranchContext.connect(url, backend="interval")
    try:
        # Intentionally fails on an existing table; never reset an application DB.
        ctx.db.execute(
            "CREATE TABLE tickets (id INTEGER PRIMARY KEY, subject TEXT, priority TEXT)"
        )
        ctx.db.execute("INSERT INTO tickets VALUES (101, 'Payment outage', 'normal')")
        ctx.db.execute(
            "INSERT INTO tickets VALUES (102, 'Documentation typo', 'normal')"
        )
        ctx.db.commit()
        ctx.register_table("tickets", ["id"])
        ctx.create_checkpoint("tickets_baseline", branch="main")
    finally:
        ctx.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--metadata-url")
    parser.add_argument("--prompts", type=Path)
    args = parser.parse_args()
    initialize(args.database_url, args.metadata_url)
    if args.prompts:
        import pandas as pd

        pd.DataFrame(
            [
                {
                    "data_source": "chronos_tickets",
                    "prompt": [{"role": "user", "content": PROMPT}],
                    "ability": "database_tools",
                    "reward_model": {
                        "style": "rule",
                        "ground_truth": "ticket_101_high_only",
                    },
                    "extra_info": {"index": 0, "split": "train"},
                    "agent_name": "chronos_db_agent",
                }
            ]
        ).to_parquet(args.prompts, index=False)


if __name__ == "__main__":
    main()
