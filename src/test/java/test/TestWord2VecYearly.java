package test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.models.word2vec.wordstore.VocabCache;
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

public class TestWord2VecYearly {
	Logger log = LoggerFactory.getLogger(getClass().getName());

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void test() throws IOException, InterruptedException, TimeoutException {
		int s = 2006;
		int e = 2017;
		String[] words = { "bank", "finance", "fintech", "latest", "modern", "money", "chat", "startup" };
		Calendar st = Calendar.getInstance();
		Calendar en = Calendar.getInstance();
		for (int i = s; i <= e; i++) {
			st.setTimeZone(TimeZone.getTimeZone("UTC"));
			st.set(i, 0,  1, 0, 0, 0);
			st.set(Calendar.MILLISECOND, 0);
			en = (Calendar) st.clone();
			en.add(Calendar.YEAR, 1);
			long stu = (long) Math.floor(st.getTimeInMillis() / 1000);
			long enu = (long) Math.floor(en.getTimeInMillis() / 1000);
			log.info("Getting result for " + stu + " .. " + enu);
			dumpPeriod("year-" + i, stu, enu, words);
		}
	}

	public void dumpPeriod(String label, long start, long end, String[] words) throws IOException, InterruptedException, TimeoutException {
		String sql = "select title, text from[bigquery-public-data:hacker_news.full]"
				+ " where parent is null"
				+ " and time is not null and text is not null and text != ''"
				+ " and time >= " + start + " and time <= " + end
				+ " and dead is null or dead is false"
				+ " and deleted is null or deleted is false"
				+ " order by time asc "
				+ " limit 30000";
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
		log.info("Running job, query: " + sql);
		//JobConfiguration = JobConfiguration.Builder<., Builder<T,B>>
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
		queryJob = queryJob.waitFor();
		log.info("Getting job result");
		QueryResponse response = bigquery.getQueryResults(jobId);
		QueryResult result = response.getResult();

		LinkedList<String> ll = new LinkedList<>();
		StringBuffer topic = new StringBuffer();
		while (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				topic = new StringBuffer();
				for (FieldValue val : row) {
					String sss = val.getStringValue();
					if (sss != null) {
						sss = sss.trim().replaceAll("\\<[^>]*>","");
						sss = StringCleaning.stripPunct(sss);
						topic.append(sss);
						topic.append(" ");
					}
				}
				ll.add(topic.toString());
			}
			result = result.getNextPage();
		}
		
		log.info("Got " + ll.size() + " lines in query result");
		if (ll.size() > 0) {
	        log.info("Load & Vectorize Sentences....");
	        SentenceIterator iter = new ListSequenceIterator(ll);
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

	        VocabCache<VocabWord> vocab = vec.getVocab();
	        
	        for (int i = 0; i < words.length; i++) {
		        Collection<String> lst = vec.wordsNearest(words[i], 10);
				VocabWord tt = vocab.tokenFor(words[i]);
				long wc = tt != null ? tt.getSequencesCount() : 0;
		        StringBuilder ss = new StringBuilder();
		        ss.append("[ \"" + label + "\", \"" + words[i] + "\", " + wc);
		        for (Iterator<String> it = lst.iterator(); it.hasNext();) {
		        	String nn = it.next();
		        	ss.append(", \"");
					ss.append(nn);
					ss.append("\", ");
					ss.append(vocab.tokenFor(nn).getSequencesCount());
		        }
		        ss.append("], ");
		        System.out.println(ss);
	        }
		}
	}

}
