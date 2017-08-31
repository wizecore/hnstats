package test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWord2VecDB extends BaseUtil {
	Logger log = LoggerFactory.getLogger(getClass().getName());
	
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws SQLException, IOException {
		opendb();
		
		Statement sql = conn.createStatement();
		int s = 2006;
		int e = 2011;
		String[] words = { "bank", "finance", "fintech", "latest", "modern", "money", "chat", "startup" };
		StringBuilder out = new StringBuilder();
		
		for (int i = s; i <= e; i++) {
			Calendar st = Calendar.getInstance();
			Calendar en = Calendar.getInstance();
			st.setTimeZone(TimeZone.getTimeZone("UTC"));
			st.set(i, 0,  1, 0, 0, 0);
			st.set(Calendar.MILLISECOND, 0);
			en = (Calendar) st.clone();
			en.add(Calendar.YEAR, 1);
			long stu = (long) Math.floor(st.getTimeInMillis() / 1000);
			long enu = (long) Math.floor(en.getTimeInMillis() / 1000);
			log.info("Getting result for " + i + ", range " + stu + " .. " + enu);
			int year = i;
			ResultSet rs = sql.executeQuery("select count(*) from hnews where time >= " + stu + " and time <= " + enu + " and (dead = false or dead is null) and (deleted = false or deleted is null)");
			if (rs != null && rs.next()) {
				log.info("Count: " + rs.getInt(1));
			}
			
			log.info("Reading data...");
			rs = sql.executeQuery("select type, title, text, id from hnews where time >= " + stu + " and time <= " + enu + " and (dead = false or dead is null) and (deleted = false or deleted is null)");
			LinkedList<String> ll = new LinkedList<>();
			FileOutputStream fos = new FileOutputStream("data-" + year + ".tsv");
			long chars = 0;
			OutputStreamWriter tsv = new OutputStreamWriter(fos, "UTF-8");
			int count = 0;
			while (rs != null && rs.next()) {
				String title = rs.getString(2);
				String text = rs.getString(3);
				long id = rs.getLong(4);
				StringBuilder topic = new StringBuilder();
				parseValue(topic, title);
				parseValue(topic, text);
				tsv.write(String.valueOf(id));
				tsv.write("\t");
				String l = topic.toString();
				tsv.write(l);
				tsv.write("\n");
				ll.add(l);
				chars += l.length();
				count ++;
				if (count % 10000 == 0 && count > 0) {
					log.info("Ready " + count + " lines");
				}
			}
			
			tsv.close();
			fos.close();
			
			log.info("Got " + ll.size() + " lines, " + chars + " chars");
			StringBuilder lout = new StringBuilder();
			learn("year-" + year, words, ll, lout);
			System.out.println(lout);
			out.append(lout.toString());
		}
		
		FileOutputStream fos = new FileOutputStream("data.json");
		OutputStreamWriter fw = new OutputStreamWriter(fos, "UTF-8");
		fw.write("[\n");
		fw.write(out.toString());
		fw.write("]\n");
		fw.close();
		fos.close();
		
		closedb();
	}

}
