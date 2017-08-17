package test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.StringCleaning;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.deeplearning4j.ui.api.UIServer;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;

public class TestWord2Vec {
	Logger log = LoggerFactory.getLogger(getClass().getName());

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws IOException, InterruptedException, TimeoutException {
		String sql = "select text from[bigquery-public-data:hacker_news.full]"
				+ " where parent is null"
				+ " and time is not null and text is not null and text != ''"
				+ " order by time asc "
				+ " limit 10000";
		log.info("Building query config");
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).
			      build();
		log.info("Building BigQuery engine");
		String projectId = "testhn-177017";
		if (System.getenv("PROJECTID") != null) {
			projectId = System.getenv("PROJECTID");
		}
		BigQuery bigquery = BigQueryOptions.newBuilder()
				.setProjectId(projectId)
				.setCredentials(GoogleCredentials.fromStream(new FileInputStream("google-creds.json")))
				.build()
				.getService();
		log.info("Declaring job");
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		log.info("Running job");
		//JobConfiguration = JobConfiguration.Builder<., Builder<T,B>>
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
		queryJob = queryJob.waitFor();
		log.info("Getting job result");
		QueryResponse response = bigquery.getQueryResults(jobId);
		QueryResult result = response.getResult();

		LinkedList<String> ll = new LinkedList<>();
		while (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				for (FieldValue val : row) {
					String sss = val.getStringValue();
					if (sss != null) {
						sss = sss.trim().replaceAll("\\<[^>]*>","");
						sss = StringCleaning.stripPunct(sss);
						ll.add(sss);
					}
				}
			}
			result = result.getNextPage();
		}
		
		log.info("Got " + ll.size() + " lines in query result");
		FileOutputStream fos = new FileOutputStream("raw.txt");
		fos.write(ll.toString().getBytes("UTF-8"));
		fos.close();
		
        log.info("Load & Vectorize Sentences....");
        // Strip white space before and after for each line
        SentenceIterator iter = new ListSequenceIterator(ll);
        // SentenceIterator iter = new BasicLineIterator("raw.txt");
        TokenizerFactory t = new DefaultTokenizerFactory();
        t.setTokenPreProcessor(new CommonPreprocessor());
        
        log.info("Building model....");
        Word2Vec vec = new Word2Vec.Builder()
                .minWordFrequency(5)
                .iterations(1)
                .layerSize(100)
                .seed(System.currentTimeMillis())
                .windowSize(5)
                .iterate(iter)
                .tokenizerFactory(t)
                .build();

        log.info("Fitting Word2Vec model....");
        vec.fit();
        
        String[] words = { "bank", "finance", "fintech", "latest", "modern", "money", "chat", "startup" };
        for (int i = 0; i < words.length; i++) {
	        Collection<String> lst = vec.wordsNearest(words[i], 10);
	        System.out.println(words[i] + ": " + lst);
        }
        
        UIServer server = UIServer.getInstance();
        System.out.println("Started on port " + server.getPort());
        Thread.sleep(10000000l);
	}

}
