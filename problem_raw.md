The problem
An online shop logs page views, add-to-carts, purchases, and ad clicks as CSV files. The logs are dirty, with duplicates, malformed lines, bad timestamps, and late events. One shopper can have many user IDs, and a third file links them across long chains and shared devices.

Clean the logs in /data, join each person's IDs, build sessions, credit each session to the ad click that brought the shopper in, count assisting campaigns from the week before, and write two daily Parquet tables (/output/user_daily/ and /output/campaign_daily/), one per person and one per campaign. The rules are exact so there is one right answer, and a fast answer that is almost right scores nothing.

Your score is container run time in seconds, and lower is better. You may submit 2 times an hour on hidden practice data (about 20 million events). The final runs on about 60 million events and caps each run at 3 minutes: if any final run goes over, you get no final score, so aim for under about 60 seconds in practice.

Start locally
Download rules.md and a starter below (PySpark, DuckDB, or Polars). Any engine, any language, any amount of AI: Spark, DuckDB, Polars, Rust, C++, plain Python.

Each starter writes both tables with the right columns but no rows, so implementing the rules is up to you.

Implement the rules.md pipeline and pass the starter's 36 edge-case fixtures.
Generate the 1M-event dev set with generator.zip, unzip dev-answer.zip, and run make check-dev to check your output locally.
What to hand in
Submit a public GitHub repository with a Dockerfile at its root and the commit to score. Keep the repository at most 50 MB with no data checked in, because the dataset is mounted at run time.

Container limits
Your image must be linux/amd64 and at most 8 GB. The build has network access for up to 10 minutes; runs have no network.
Each run gets 8 pinned vCPUs (hyperthreads), 24 GB of memory, and no swap.
/data is read-only, and /scratch is writable disk for spills. /output and /scratch together may hold at most 50 GB.
The root filesystem is read-only except for /tmp (64 MB in memory), and your process runs as uid 1000.
Environment variables: DATA_DIR=/data, OUTPUT_DIR=/output, SCRATCH_DIR=/scratch, HOME=/scratch, NCPU=8, and MEM_GB=24.
At most 4,096 processes and threads, and at most 50 MB on stdout (log to stderr).

Scoring
Metric
Run time, lower is better
Baseline to beat
172.00 s (Starter: PySpark, local mode)
Formula
one run on hidden data (median of 3 under 20 s), start to exit
Ties
Equal scores rank the earlier submission first.
Scorer version
shopping-sessions-4
We build your Dockerfile, check it on the dev set, and time it once in a fresh container on hidden practice data (about 20 million events). Runs under 20 seconds are timed 3 times and scored on the median.

Rules
Where a starter and rules.md disagree, rules.md wins. Ask the organizers if rules.md leaves you unsure.
Each submission runs first on the public 1M-event dev set (at most 3 minutes). If it fails there, you get the differing rows and your logs, and it stops.
A practice run may take at most 2× the baseline's time and never more than 5 minutes. It is timed once, or 3 times if the first run is under 20 seconds; repeats share the first run's time limit, and a repeat that is cut off counts as the full limit.
A wrong table, a crash, or hitting the memory, disk, stdout, or time limit stops the submission.
The final uses about 3× the practice data and caps each run at 3 minutes. Any final run over 3 minutes gives no final score, so aim for under about 60 seconds in practice.
Practice times can vary by up to about 20% across scoring machines, so small gaps on the practice board are not meaningful.
Hidden runs never show your logs or values from the data: only the exit code, whether memory ran out, and counts of missing, extra, and differing rows.
The clock runs from container start to container exit, including starting your engine and reading CSVs.
Your team may submit 2 times an hour. Rejected submissions never change your best score.
Keep your repository public, with every commit you submitted still in it, until results are published. An entry we cannot fetch or restore for the final scores nothing.
The final
At the cutoff, we rebuild your chosen commit (your best, unless you pick another) and time it 3 times on about 60 million new events made after submissions close, each run capped at 3 minutes. If any run goes over 3 minutes, you get no final score; otherwise your score is the median. At least the 10 fastest entries are then retimed 7 times alone on one idle machine, and that median replaces their first score.

The cutoff is Sun 10:00 AM CST.