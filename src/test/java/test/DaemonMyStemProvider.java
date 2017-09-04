package test;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import ru.stachek66.nlp.mystem.holding.Factory;
import ru.stachek66.nlp.mystem.holding.MyStem;
import ru.stachek66.nlp.mystem.holding.MyStemApplicationException;
import ru.stachek66.nlp.mystem.holding.Request;
import ru.stachek66.nlp.mystem.model.Info;
import scala.Option;
import scala.collection.JavaConversions;

public class DaemonMyStemProvider {
	private Logger log = Logger.getLogger(getClass().getName());
	
	private MyStem mystemAnalyzer = null;
	
	private String mystemPath = null;
	
	public String mystem(String text) throws IOException {
		if (mystemAnalyzer == null) {
			File f = mystemPath != null ? new File(mystemPath) : null;
			mystemAnalyzer = (MyStem) new Factory("-igd --eng-gr --format json --weight").newMyStem("3.0", Option.apply(f)).get();
		}
		
		try {
			StringTokenizer tk = new StringTokenizer(text, ",\n\r\t ;.-");
			StringBuilder b = new StringBuilder();
			while (tk.hasMoreTokens()) {
				// Single lex term per token
				String token = tk.nextToken();
				boolean haveToken = false;
				final Iterable<Info> result = once(token);
	            for (final Info info : result) {
	                if (!info.lex().isEmpty()) {
	                	b.append(info.lex().get());
		                b.append(" ");
		                haveToken = true;
		                break;
	                }
	            }
	            
	            if (!haveToken) {
	            	// Some number?
	            	b.append(token);
	            	b.append(" ");
	            }
			}
    		
            text = b.toString().trim();
            log.info("Lemmatized: " + text);
            return text;
		} catch (Exception e) {
			throw new IOException(e.getMessage() != null ? e.getMessage() : e.toString(), e);
		}
	}

	protected Iterable<Info> once(String text) throws MyStemApplicationException {
		final Iterable<Info> result = JavaConversions.asJavaIterable(
		                mystemAnalyzer
		                        .analyze(Request.apply(text))
		                        .info()
		                        .toIterable());
		return result;
	}
}
