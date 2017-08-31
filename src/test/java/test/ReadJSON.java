package test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class ReadJSON extends BaseUtil {

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws IOException, SQLException {
		opendb();
		PreparedStatement ps = conn.prepareStatement(
				"insert into hnews ("
				+ "by, score, time, timestamp, title, type, url, text, parent, deleted, dead, "
				+ "descendants, id, ranking) values ("
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
				+ ")");
		Statement st = conn.createStatement();
		String file = System.getenv("HNJSON_FILE");
		GsonBuilder b = new GsonBuilder();
		Gson gson = b.create();
		int lc = 0;
		try (FileInputStream fis = new FileInputStream(file)) {
			InputStreamReader r = new InputStreamReader(fis, "UTF-8");
			String line = null;
			BufferedReader in = new BufferedReader(r);
			while ((line = in.readLine()) != null) {
				JsonObject o = gson.fromJson(line, JsonObject.class);
				JsonElement text = o.get("body").getAsJsonObject().get("text");
				text = text == null ? gson.fromJson("\"\"", JsonPrimitive.class) : text;
				//System.out.println(lc + ": " + text.getAsString());
				lc ++;
				gulpone(st, ps, o);
				if (lc % 10000 == 0 && lc > 0) {
					System.out.println("Done " + lc + " rows, maxid: " + maxid);
				}
			}
		}
		closedb();
	}
	
	private String strv(JsonObject o, String key, String def) {
		JsonElement e = o.get(key);
		String v = null;
		if (e != null) {
			v = e.getAsString();
		}
		if (v == null) {
			return def;
		} else {
			return v;
		}
	}

	private Long longv(JsonObject o, String key, Long def) {
		JsonElement e = o.get(key);
		Long v = null;
		if (e != null && !e.isJsonNull()) {
			v = e.getAsLong();
		}
		if (v == null) {
			return def;
		} else {
			return v;
		}
	}

	private void gulpone(Statement st, PreparedStatement ps, JsonObject o) throws SQLException {
		long id = o.get("id").getAsLong();
		if (id <= maxid) {
			return;
		}
		
		JsonObject b = o.get("body").getAsJsonObject();
		int i = 1;
		ps.setObject(i++, strv(b, "by", null)); // by
		ps.setObject(i++, longv(b, "score", null)); // score
		ps.setObject(i++, longv(b, "time", null)); // time
		ps.setObject(i++, null); // timestamp_ts UNAVAILABLE IN JSON DUMP
		ps.setObject(i++, strv(b, "title", null)); // title
		ps.setObject(i++, strv(b, "type", null)); // type
		ps.setObject(i++, strv(b, "url", null)); // url
		ps.setObject(i++, strv(b, "text", null)); // text
		ps.setObject(i++, longv(b, "parent", null)); // parent
		ps.setObject(i++, false); // deleted UNAVAILABLE IN JSON DUMP
		ps.setObject(i++, false); // dead UNAVAILABLE IN JSON DUMP
		ps.setObject(i++, longv(b, "descendants", null)); // descendants
		ps.setObject(i++, id); // id
		ps.setObject(i++, null); // ranking UNAVAILABLE IN JSON DUMP
		ps.execute();
		maxid = id;
		// log.info("Maxid: " + maxid);
		
		st.execute("update conf set maxid = " + maxid);
	}

}
