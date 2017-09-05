![HackerNews analytics](hnstats-ewan-robertson-208059.png)

Using available [HackerNews](https://news.ycombinator.com) dataset produce some insight into the most meaningful topics.

## Ultimate reason

  * Most discussed topics (and yearly shift)
  * Top technology and startups

## Technology behind it

  * Java
  * Deeplearning4J
  * Word2vec
  * Stanford CoreNLP (lemmatizing)

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
