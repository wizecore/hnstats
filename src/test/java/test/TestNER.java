package test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Test;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;

public class TestNER extends BaseUtil {

	@After
	public void tearDown() throws Exception {
	}
	
	/**
	 * ResultSet rs = null;
		Stream<Object> stream = StreamSupport.stream(new Spliterator<Object>() {
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ClassCastException 
	 * @throws SQLException 
			@Override
			public int characteristics() {
				return IMMUTABLE | CONCURRENT;
			}
			
			@Override
			public long estimateSize() {
				// TODO Auto-generated method stub
				return Long.MAX_VALUE;
			}
			
			@Override
			public boolean tryAdvance(Consumer<? super Object> action) {
				try {
					if (!rs.next()) {
						return false;
					}
					action.accept(new Object());
					return true;
				} catch (SQLException e) {
					e.printStackTrace();
					return false;
				}
			}
			
			@Override
			public Spliterator<Object> trySplit() {
				return null;
			}
		}, true);
		stream.
	 */

	@Test
	public void test() throws ClassCastException, ClassNotFoundException, IOException, SQLException {
		String serializedClassifier = "classifiers/english.all.3class.distsim.crf.ser.gz";
		AbstractSequenceClassifier<CoreLabel> cls = CRFClassifier.getClassifier(serializedClassifier);
		opendb();
		Statement sql = conn.createStatement();
		long stu = 1356998400;
		long enu = 1388534400;
		ResultSet rs = sql.executeQuery("select type, title, text, id from hnews where time >= " + stu + " and time <= " + enu + " and (dead = false or dead is null) and (deleted = false or deleted is null)");
		while (rs.next()) {
			String title = rs.getString(2);
			String text = rs.getString(3);
			long id = rs.getLong(4);
			if (title != null && text != null) {
				System.out.print(id);
				System.out.print("\t");
				String str = title + "." + text;
				System.out.println(cls.classifyWithInlineXML(str));
			}
		}
		closedb();
	}

}
