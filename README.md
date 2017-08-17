# HackerNews analytics
Using Google BigQuery public dataset produce some insight into the most discussed topics.

## Roadmap
- Gather data (DONE)
- Produce JSON (DONE)
- For selected terms - related words trending through years 2007 - 2017 (DONE)
- All terms - counts every year (TODO)
- Term cleanup (TODO)

## Demo
http://гайнутдинов.рф/hnstats/terms.html

Demo is single page HTML with all data precomputed and embedded.

## Source repo
https://github.com/huksley/hnstats

## Building manually

1. Create Google Cloud project & add BigQuery perms
2. Fork this repo
3. Fine tune queries inside code (terms, periods)
4. Produce data.json
5. Modify terms.json
