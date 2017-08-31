# HackerNews analytics
Using Google BigQuery public dataset produce some insight into the most discussed topics.

## Roadmap
- Gather data (DONE)
- Produce JSON (DONE)
- For selected terms - related words trending through years 2007 - 2017 (DONE)
- All terms - counts every year (TODO)
- Term cleanup (TODO)
- Auto build SPA, i.e. push -> CI -> deploy (TODO)
- Fine tune word2vec params (see below)

## Demo
http://wizecore.com/hnstats/terms.html

Demo is single page HTML with all data precomputed and embedded.

## Source repo
https://github.com/huksley/hnstats

## Building manually

1. Create Google Cloud project & add BigQuery perms
2. Fork this repo
3. Fine tune queries inside code (terms, periods)
4. Produce data.json
5. Modify terms.json

## Word2vec tuning

**Help is welcome** in fine-tuning word2vec parameters. Here is current setup:

```java
Word2Vec vec = new Word2Vec.Builder()
                .minWordFrequency(5)
                .iterations(1)
                .layerSize(100)
                .seed(System.currentTimeMillis())
                .windowSize(5)
                .iterate(iter)
                .tokenizerFactory(t)
                .build();
```
