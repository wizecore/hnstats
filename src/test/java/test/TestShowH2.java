package test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestShowH2 {
	Logger log = LoggerFactory.getLogger(getClass().getName());

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws ClassNotFoundException, SQLException, InterruptedException, TimeoutException, IOException {
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.getConnection("jdbc:h2:./hndump");
		Statement st = conn.createStatement();
		try { 
			try {
				st.execute("CREATE USER admin2 PASSWORD '123' admin;");
			} catch (Exception e) {
				e.printStackTrace();
			}
			ResultSet rs = st.executeQuery("select * from hnews");
			int row = 0;
			while (rs.next()) {
				log.info("Row: " + row + ", title: " + rs.getString("title") + ", " + rs.getString("type") + ", " + rs.getString("url"));
				row ++;
			}
		} catch (SQLException e) {
			// e.printStackTrace();
		}
		conn.close();
	}
}
