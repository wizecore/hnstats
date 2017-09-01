package test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.ServiceAccountSigner;
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
import com.google.common.util.concurrent.RateLimiter;

public class TestWord2VecYearly extends BaseUtil {
	Logger log = LoggerFactory.getLogger(getClass().getName());
	RateLimiter qrate = RateLimiter.create(500);

	@After
	public void tearDown() throws Exception {
	}
	
	
	@Test
	public void test() throws IOException, InterruptedException, TimeoutException, ExecutionException {
		ExecutorService threads = null; 
		// threads = Executors.newFixedThreadPool(3);
		int s = 2012;
		int e = 2017;
		String[] words = { "bank", "finance", "fintech", "latest", "modern", "money", "chat", "startup" };
		Calendar st = Calendar.getInstance();
		Calendar en = Calendar.getInstance();
		StringBuilder out = new StringBuilder();
		for (int i = s; i <= e; i++) {
			st.setTimeZone(TimeZone.getTimeZone("UTC"));
			st.set(i, 0,  1, 0, 0, 0);
			st.set(Calendar.MILLISECOND, 0);
			en = (Calendar) st.clone();
			en.add(Calendar.YEAR, 1);
			long stu = (long) Math.floor(st.getTimeInMillis() / 1000);
			long enu = (long) Math.floor(en.getTimeInMillis() / 1000);
			log.info("Getting result for " + i + ", range " + stu + " .. " + enu);
			int year = i;
			
			Runnable run = () -> {
				try {
					dumpPeriod("year-" + year, stu, enu, words, out);
				} catch (Exception ex) {
					ex.printStackTrace();
					log.error("year-" + year + ": failed: " + ex.toString());
				}
			};
			
			if (threads != null) {
				Future<?> submit = threads.submit(run);
				submit.get();
			} else {
				run.run();
			}
		}
		
		if (threads != null) {
			threads.shutdown();
			threads.awaitTermination(24, TimeUnit.HOURS);
		}
		
		System.out.println("[");
		System.out.print(out);
		System.out.println("]");
		
		FileOutputStream fos = new FileOutputStream("data.json");
		OutputStreamWriter fw = new OutputStreamWriter(fos, "UTF-8");
		fw.write("[\n");
		fw.write(out.toString());
		fw.write("]\n");
		fw.close();
		fos.close();
	}

	/**
	 * - Get root topics
	 * - Get first line of comments
	 * 
	 * @param label
	 * @param start
	 * @param end
	 * @param words
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws TimeoutException
	 */
	public void dumpPeriod(String label, long start, long end, String[] words, StringBuilder out) throws IOException, InterruptedException, TimeoutException {
		String sql = "select id, title, text, descendants from[bigquery-public-data:hacker_news.full]"
				+ " where parent is null"
				+ " and time is not null"
				+ " and time >= " + start + " and time <= " + end
				+ " and dead is null or dead is false"
				+ " and deleted is null or deleted is false"
				+ " order by id asc "
				+ " limit 1000000";
		qrate.acquire(100000);
		QueryResult result = query(label, sql);
		
		LinkedList<Long> ids = new LinkedList<>();
		LinkedList<String> ll = new LinkedList<>();
		
		FileOutputStream fos = new FileOutputStream("out-" + label + "-top.tsv");
		OutputStreamWriter fw = new OutputStreamWriter(fos, "UTF-8");
		
		long st = System.currentTimeMillis(); 
		//qrate.acquire((int) result.getTotalRows());
		long en = System.currentTimeMillis(); 
		log.info("Waited for query rate " + (en - st) + "ms");
		
		parse(label, result, ids, ll, fw);
		fw.close();
		fos.close();
		
		log.info(label + ": Got " + ll.size() + " lines in top result, " + ids.size() + " ids");
		
		int b = 10000;
		int done = 0;
		int batch = 1;
		log.info(label + ": Query top comments, batches of " + b);
		while (ids.size() > 0) {
			List<Long> l = ids.subList(0, b > ids.size() ? ids.size() : b);
			StringBuilder q = new StringBuilder();
			for (Long id: l) {
				if (q.length() > 0) {
					q.append(", ");
				}
				q.append(id);
			}
			done += l.size();
			l.clear();
			
			sql = "select id, title, text, descendants from[bigquery-public-data:hacker_news.full]"
					+ " where parent in (" + q + ")"
					+ " and time is not null"
					+ " and time >= " + start + " and time <= " + end
					+ " and dead is null or dead is false"
					+ " and deleted is null or deleted is false"
					+ " order by id asc "
					+ " limit 100000";
			qrate.acquire(100000);
			result = query(label, sql);
			log.info(label + ": Total rows in batch: " + result.getTotalRows());
			
			fos = new FileOutputStream("out-" + label + "-batch-" + batch + ".tsv");
			fw = new OutputStreamWriter(fos, "UTF-8");
			
			st = System.currentTimeMillis(); 
			//qrate.acquire((int) result.getTotalRows());
			en = System.currentTimeMillis(); 
			log.info("Waited for query rate " + (en - st) + "ms");
			
			// Don`t keep IDs, parse only top level
			parse(label, result, new LinkedList<Long>(), ll, fw);
			fw.close();
			fos.close();
			
			log.info(label + ": Batch done " + done + ", rest " + ids.size());
			batch ++;
		}
		
		log.info(label + ": Total text lines " + ll.size());
		if (ll.size() > 0) {
	        learn(label, words, ll, null, out);
		}
	}

	public void parse(String label, QueryResult result, LinkedList<Long> ids, LinkedList<String> ll, Writer out) throws IOException {
		log.info(label + ": Parsing " + result.getTotalRows() + " rows");
		while (result != null) {
			long lastSeen = 0;
			for (List<FieldValue> row : result.iterateAll()) {
				long id = Long.parseLong(row.get(0).getStringValue());
				if (id > lastSeen) {
					ids.add(id);
					StringBuilder topic = new StringBuilder();
					// title
					parseValue(topic, unfv(row.get(1)));
					// text
					parseValue(topic, unfv(row.get(2)));
					
					if (out != null) {
						out.write(String.valueOf(id));
						out.write("\t");
						out.write(topic.toString());
						out.write("\n");
					}
					
					ll.add(topic.toString());
					lastSeen = id;
				}
			}
			result = result.getNextPage();
		}
	}
	

	private String unfv(FieldValue fv) {
		return fv != null ? fv.getStringValue() : null;
	}


	public QueryResult query(String label, String sql)
			throws IOException, FileNotFoundException, InterruptedException, TimeoutException {
		synchronized (threadLock) {
			log.info(label + ": Building query config");
			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
			log.info(label + ": Building BigQuery engine");
			String projectId = System.getenv("PROJECTID");
			GoogleCredentials cred = GoogleCredentials.fromStream(new FileInputStream("google-creds.json"));
			if (projectId == null && cred instanceof ServiceAccountSigner) {
				ServiceAccountSigner s = (ServiceAccountSigner) cred;
				projectId = s.getAccount().replaceAll("^.*@", "").replaceAll("\\..*", "");
			}
			
			// "testhn-177017";
			BigQuery bigquery = BigQueryOptions.newBuilder()
					.setProjectId(projectId)
					.setCredentials(cred)
					.build()
					.getService();
			log.info(label + ": Declaring job");
			JobId jobId = JobId.of(UUID.randomUUID().toString());
			log.info(label + ": Running job " + jobId);
			Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
			queryJob = queryJob.waitFor();
			log.info(label + ": Getting job results");
			QueryResponse response = bigquery.getQueryResults(jobId);
			QueryResult result = response.getResult();
			return result;
		}
	}


}
