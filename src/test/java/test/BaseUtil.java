package test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.sequencevectors.SequenceVectors;
import org.deeplearning4j.models.sequencevectors.enums.ListenerEvent;
import org.deeplearning4j.models.sequencevectors.interfaces.VectorsListener;
import org.deeplearning4j.models.word2vec.VocabWord;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.models.word2vec.wordstore.VocabCache;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseUtil {
	protected Logger log = LoggerFactory.getLogger(getClass().getName());
	long maxid;
	Connection conn;
	
	public void prepare() {
		System.getProperties().put( "proxySet", "true" );
		System.getProperties().put( "socksProxyHost", "127.0.0.1" );
		System.getProperties().put( "socksProxyPort", "3128" );
		
	}
	
	public String timeLine(String objectName, long start, int count) {
		long now = System.currentTimeMillis();
		return "Elapsed " + (now - start)/1000 + "s, avg " +  Math.round(1000.0 * count / (now - start)) + " " + objectName + "/s";
	}
	
	public void opendb() throws SQLException {
		try {
			Class.forName("org.h2.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		conn = DriverManager.getConnection("jdbc:h2:./hndump");
		Statement st = conn.createStatement();
		try { 
			st.execute("create table conf (maxid numeric(15))");
		} catch (SQLException e) {
			// e.printStackTrace();
		}
		
		try {
			InputStream is = getClass().getResourceAsStream("hnews.sql");
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			int c = 0;
			byte[] buf = new byte[2048];
			while (c >= 0) {
				c = is.read(buf);
				if (c > 0) {
					os.write(buf, 0, c);
				}
			}
			String sql = new String(buf, "UTF-8");
			st.execute(sql);
		} catch (Exception e) {
			// e.printStackTrace();
		}
		
		boolean reset = false; 
		if (reset) {
			try {
				st.execute("delete from hnews");
				st.execute("update conf set maxid = 0");
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
		
		try {
			st.execute("alter table hnews alter column url varchar(8192) null");
			st.execute("create index if not exists ix_parent on hnews (parent)");
			st.execute("create index if not exists ix_id on hnews (id)");
			st.execute("create index if not exists ix_time on hnews (time)");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		maxid = new Long(0);
		ResultSet rs = st.executeQuery("select maxid from conf");
		if (rs != null && rs.next()) {
			maxid = rs.getLong(1);
		} else {
			st.execute("insert into conf (maxid) values (0)");
		}
		
		long rows = 0;
		rs = st.executeQuery("select count(*) from hnews");
		if (rs != null && rs.next()) {
			rows = rs.getInt(1);
		}
		
		if (maxid < rows) {
			rs = st.executeQuery("select max(id) from hnews");
			if (rs != null && rs.next()) {
				log.info("Seeking for maxid, rows more");
				maxid = rs.getLong(1);
			}
		}
		
		log.info("Last ID: " + maxid + ", rows: " + rows);
	}
	
	public void closedb() throws SQLException {
		conn.close();
	}
	
	StanfordLemmatizer lem = new StanfordLemmatizer();
	
	public String lemmatize(String s) {
		return lem.lemmatizeText(s);
	}
	
	public String tokenize(String terms, Collection<String> aliases) throws IOException {
		if (terms == null) {
			return null;
		}
		TokenizerFactory t = new DefaultTokenizerFactory();
		t.setTokenPreProcessor(new CommonPreprocessor());
		Tokenizer tk = t.create(terms);
		StringBuilder buf = new StringBuilder();
		while (tk.hasMoreTokens()) {
			String w = tk.nextToken();
			if (aliases != null) {
				if (aliases.contains(w)) {
					w = w.replace("-", "");
				}
			}
			if (buf.length() > 0) {
				buf.append(" ");
			}
			buf.append(w);
		}
		return buf.toString();
	}

	public Set<String> readHyphenatedAliases() throws FileNotFoundException, UnsupportedEncodingException, IOException {
		Set<String> aliases = new HashSet<>();
		FileInputStream fis = new FileInputStream("hyphenated-alias.txt");
		BufferedInputStream fisb = new BufferedInputStream(fis, 1024 * 1024);
		InputStreamReader isr = new InputStreamReader(fisb, "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String w = null;
		while ((w = br.readLine()) != null) {
			aliases.add(w);
		}
		fis.close();
		return aliases;
	}

	Pattern tags = Pattern.compile("\\<[^>]*>");
	Pattern urls = Pattern.compile( "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)", Pattern.CASE_INSENSITIVE);
	Pattern punctuation = Pattern.compile("[\\d\\.:,\"\'\\(\\)\\[\\]|/?!;]+");
	Pattern email = Pattern.compile("([^.@\\s]+)(\\.[^.@\\s]+)*@([^.@\\s]+\\.)+([^.@\\s]+)");
	Pattern spaces = Pattern.compile("[ ]+");
	Pattern entities = Pattern.compile("\\&\\#[0-9a-zA-Z]*;");
	
	public void parseValue(StringBuilder topic, String sss) {
		if (sss != null) {
			// Somehow bigdata result have / replaced by &#x2f; in urls
			sss = sss.replace("&#x2F;", "/");
			sss = tags.matcher(sss).replaceAll(" ");
			sss = urls.matcher(sss).replaceAll(" ");
			// sss = email.matcher(sss).replaceAll(" ");
			sss = sss.replace("&gt;", ">");
			sss = sss.replace("&lt;", "<");
			sss = sss.replace("&amp;", "&");
			sss = sss.replace("&nbsp;", " ");
			sss = sss.replace(" nbsp;", " ");
			sss = sss.replace("&quot;", "\"");
			sss = entities.matcher(sss).replaceAll(" ");
			sss = punctuation.matcher(sss).replaceAll(" ");
			sss = sss.replace("#", " ");
			sss = sss.replace("$", " ");
			sss = sss.replace("co-founder", "cofounder");
			sss = sss.replace("Co-Founder", "cofounder");
			sss = sss.replace("_", " ");
			sss = sss.replace("n’t", "not");
			sss = sss.replace("…", " ");
			sss = sss.replace("‘", " ");
			sss = sss.replace("’", " ");
			sss = sss.replace("–", "-");
			sss = sss.replace("`", " ");
			sss = sss.replace("€", " ");
			sss = sss.replace("%", " ");
			sss = sss.replace("“", " ");
			sss = sss.replace("”", " ");
			sss = sss.replace("*", " ");
			sss = sss.replace("\r", " ");
			sss = sss.replace("\n", " ");
			sss = sss.replace("™", " ");
			sss = spaces.matcher(sss).replaceAll(" ");
			sss = sss.trim();
			topic.append(sss);
		}
	}
	
	/*
	public String stem(String term) throws IOException {
		if (term == null) {
			return null;
		}
	    Analyzer analyzer = new StandardAnalyzer();
	    TokenStream result = analyzer.tokenStream(null, term);
	    result = new PorterStemFilter(result);
	    result = new StopFilter(result, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
	    CharTermAttribute resultAttr = result.addAttribute(CharTermAttribute.class);
	    result.reset();
	    StringBuilder tokens = new StringBuilder();
	    while (result.incrementToken()) {
	    	if (tokens.length() > 0) {
	    		tokens.append(" ");
	    	}
	        tokens.append(resultAttr.toString());
	    }
	    return tokens.toString();
	}
	*/

	protected Object threadLock = new Object();

	/**
		https://deeplearning4j.org/word2vec#setup
	 	This configuration accepts a number of hyperparameters. A few require some explanation:
		
		batchSize is the amount of words you process at a time.
		
		minWordFrequency is the minimum number of times a word must appear in the
		corpus. Here, if it appears less than 5 times, it is not learned. Words must
		appear in multiple contexts to learn useful features about them. In very large
		corpora, it’s reasonable to raise the minimum.
		
		useAdaGrad - Adagrad creates a different gradient for each feature. Here we
		are not concerned with that.
		
		layerSize specifies the number of features in the word vector. This is equal
		to the number of dimensions in the featurespace. Words represented by 500
		features become points in a 500-dimensional space.
		
		iterations this is the number of times you allow the net to update its
		coefficients for one batch of the data. Too few iterations mean it may not
		have time to learn all it can; too many will make the net’s training longer.
		
		learningRate is the step size for each update of the coefficients, as words
		are repositioned in the feature space.
		
		minLearningRate is the floor on the learning rate. Learning rate decays as the
		number of words you train on decreases. If learning rate shrinks too much, the
		net’s learning is no longer efficient. This keeps the coefficients moving.
		
		iterate tells the net what batch of the dataset it’s training on.
		
		tokenizer feeds it the words from the current batch.

	 */
	public void learn(String label, String[] words, List<String> ll, String tsvFile, StringBuilder out) throws IOException {
		synchronized (threadLock) {
			Word2Vec vec = null;
			File modelFile = new File(label + "-model.zip");
			if (modelFile.exists()) {
				log.info(label + ": Loading model from " + modelFile);
				vec = WordVectorSerializer.readWord2VecModel(modelFile);
			} else {
				log.info(label + ": Load & Vectorize Sentences from " + tsvFile + ", size " + new File(tsvFile).length() + " bytes");
				SentenceIterator iter = ll != null ? new ListSequenceIterator(ll) : new BasicLineIterator(tsvFile);
				TokenizerFactory t = new DefaultTokenizerFactory();
				t.setTokenPreProcessor(new CommonPreprocessor());
				
				log.info(label + ": Building model...");
				vec = new Word2Vec.Builder()
					.batchSize(512)
			        .minWordFrequency(5)
			        .iterations(1)
			        .layerSize(100)
			        .seed(System.currentTimeMillis())
			        .windowSize(5)
			        .iterate(iter)
			        .tokenizerFactory(t)
			        .minLearningRate(0.001) // 
			        .setVectorsListeners(Arrays.asList(new VectorsListener[] { new VectorsListener<VocabWord>() {
			        	@Override
			        	public void processEvent(ListenerEvent event, SequenceVectors<VocabWord> sequenceVectors,
			        			long argument) {
			        	}
			        	@Override
			        	public boolean validateEvent(ListenerEvent event, long argument) {
			        		return false;
			        	}
					}}))
			        .build();

				log.info(label + ": Fitting Word2Vec model");
				vec.fit();
				
				log.info(label + ": Saving model");
				WordVectorSerializer.writeWord2VecModel(vec, modelFile);
			}

			VocabCache<VocabWord> vocab = vec.getVocab();
			for (int i = 0; i < words.length; i++) {
			    Collection<String> lst = vec.wordsNearest(words[i], 10);
				VocabWord tt = vocab.tokenFor(words[i]);
				long wc = tt != null ? tt.getSequencesCount() : 0;
			    out.append("[ \"" + label + "\", \"" + words[i] + "\", " + wc);
			    for (Iterator<String> it = lst.iterator(); it.hasNext();) {
			    	String nn = it.next();
			    	out.append(", \"");
					out.append(nn);
					out.append("\", ");
					out.append(vocab.tokenFor(nn).getSequencesCount());
			    }
			    out.append("],\n");
			}
			
			System.out.println(label + ": " + vec.wordsNearest(Arrays.asList("google", "russia"), Arrays.asList("usa"), 10));
		}
	}
}
