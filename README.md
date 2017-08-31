![HackerNews analytics](hnstats-ewan-robertson-208059.png)

Using publicically available [HackerNews](https://news.ycombinator.com) dataset produce some insight into the most discussed topics.

## Ultimate reason

  * Most discussed topics (and yearly shift)
  * Top technology and startups

## Technology behind it

  * Java
  * Deeplearning4J
  * Word2vec

## Project

[Online version](http://wizecore.com/hnstats/terms.html)

## Roadmap
- Gather data (DONE)
- Produce JSON (DONE)
- For selected terms - related words trending through years 2007 - 2017 (DONE)
- All terms - display counts every year (TODO)
- Term cleanup (DONE)
- Auto build SPA, i.e. push -> CI -> deploy (TODO)
- Fine tune word2vec params (see below)

## Source repo

Project is hosted on [GitHub](https://github.com/wizecore/hnstats)

## Building manually

1. Create Google Cloud project & add BigQuery perms
2. Fork this repo
3. Fine tune queries inside code (terms, periods)
4. Produce data.json
5. Modify terms.html

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
