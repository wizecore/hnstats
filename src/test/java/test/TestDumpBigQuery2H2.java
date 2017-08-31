package test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
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

public class TestDumpBigQuery2H2 extends BaseUtil {
	Logger log = LoggerFactory.getLogger(getClass().getName());

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws ClassNotFoundException, SQLException, InterruptedException, TimeoutException, IOException {
		opendb();
		RateLimiter qrate = RateLimiter.create(500);
		
		try {
			do {
				long newmax = dumpRows(qrate, 1000);
				if (newmax == (long) maxid) {
					// No more new IDs
					break;
				}
				maxid = newmax;
			} while (true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		closedb();
	}

	int credIndex = 1;

	public Long dumpRows(RateLimiter qrate, int batch)
			throws IOException, FileNotFoundException, InterruptedException, TimeoutException, SQLException {
		Statement st = conn.createStatement();
		String sql = "select 'by', score, time, timestamp, title, type, url, text, parent, deleted, dead, descendants, id, ranking from[bigquery-public-data:hacker_news.full]"
					+ " where id > " + maxid + " order by id limit " + batch;
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
		
		QueryResult result = null;
		do {
			String file = "google-creds" + (credIndex == 1 ? "" : String.valueOf(credIndex)) + ".json";
			String projectId = System.getenv("PROJECTID");
			GoogleCredentials cred = GoogleCredentials.fromStream(new FileInputStream(file));
			if (projectId == null && cred instanceof ServiceAccountSigner) {
				ServiceAccountSigner s = (ServiceAccountSigner) cred;
				projectId = s.getAccount().replaceAll("^.*@", "").replaceAll("\\..*", "");
			}
			
			qrate.acquire(batch);
			BigQuery bigquery = BigQueryOptions.newBuilder()
					.setProjectId(projectId)
					.setCredentials(cred)
					.build()
					.getService();
			JobId jobId = JobId.of(UUID.randomUUID().toString());
			Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
			queryJob = queryJob.waitFor();
			try {
				QueryResponse response = bigquery.getQueryResults(jobId);
				result = response.getResult();
			} catch (Exception e) {
				log.warn("Failed cred file: " + file + ", trying again (" + e + ")");
				credIndex ++;
			}
			
			if (credIndex > 10) {
				break;
			}
		} while (result == null);
		
		if (result == null) {
			throw new RuntimeException("Big query failed, abort");
		}
		
		conn.setAutoCommit(false);
		PreparedStatement ps = conn.prepareStatement("insert into hnews (by, score, time, timestamp, title, type, url, text, parent, deleted, dead, descendants, id, ranking) values ("
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
				+ ")");
		while (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				// log.info("Row: " + row);
				int i = 0;
				ps.setObject(i + 1, strv(row.get(i++))); // by
				ps.setObject(i + 1, intv(row.get(i++))); // score
				ps.setObject(i + 1, intv(row.get(i++))); // time
				ps.setObject(i + 1, null); i ++;// intv(row.get(i++))); // timestamp
				ps.setObject(i + 1, strv(row.get(i++))); // title
				ps.setObject(i + 1, strv(row.get(i++))); // type
				ps.setObject(i + 1, strv(row.get(i++))); // url
				ps.setObject(i + 1, strv(row.get(i++))); // text
				ps.setObject(i + 1, intv(row.get(i++))); // parent
				ps.setObject(i + 1, boolv(row.get(i++))); // deleted
				ps.setObject(i + 1, boolv(row.get(i++))); // dead
				ps.setObject(i + 1, intv(row.get(i++))); // descendants
				Long id = intv(row.get(i++));
				ps.setObject(i, id); // id
				ps.setObject(i + 1, intv(row.get(i++))); // ranking
				ps.execute();
				maxid = id;
				st.execute("update conf set maxid = " + maxid);
			}
			result = result.getNextPage();
		}
		log.info("Maxid: " + maxid);
		conn.commit();
		return maxid;
	}
	
	private String strv(FieldValue fv) {
		return !fv.isNull() && !fv.getStringValue().equals("") ? fv.getStringValue() : null;
	}

	private Long intv(FieldValue fv) {
		return !fv.isNull() && !fv.getStringValue().equals("") ? Long.parseLong(fv.getStringValue()) : null;
	}

	private Boolean boolv(FieldValue fv) {
		return !fv.isNull() && !fv.getStringValue().equals("") ? Boolean.parseBoolean(fv.getStringValue()) : null;
	}
}
