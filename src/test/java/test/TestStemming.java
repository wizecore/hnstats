package test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStemming extends BaseUtil {
	Logger log = LoggerFactory.getLogger(getClass().getName());
	
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testStemmer() throws IOException {
		// System.out.println(stem("the startups and startup and start-up and start-ups and email and e-mail and e-mails is just the same words over and over"));
	}
	
	Pattern hyphenWord = Pattern.compile("(?=\\S*['-])([a-zA-Z'-]+)");
	
	Count words = new Count();
	
	public class Count extends HashMap<String, Integer> {
		Integer ONE = new Integer(1);
		
	    public void add(String o) {
	    	if (containsKey(o)) {
		        int count = this.get(o) + 1;
		        super.put(o, new Integer(count));
	    	} else {
	    		super.put(o, ONE);
	    	}
	    }
	}
	
	public static <K, V extends Comparable<? super V>> Map<K, V>  sortByValue(Map<K, V> map, boolean reverse) {
	    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
	    Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
	        public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
	            if (reverse) {
	            	return (o2.getValue()).compareTo(o1.getValue());
	            } else {
	            	return (o1.getValue()).compareTo(o2.getValue());
	            }
	        }
	    });
	
	    Map<K, V> result = new LinkedHashMap<K, V>();
	    for (Map.Entry<K, V> entry : list) {
	        result.put(entry.getKey(), entry.getValue());
	    }
	    return result;
	}

	@Test
	public void testReadAllHyphen() throws IOException {
		File[] f = new File(".").listFiles();
		int c = 0;
		for (File ff: f) {
			if (ff.getName().startsWith("data-") && ff.getName().endsWith(".tsv")) {
				log.info("Reading " + ff.getName());
				FileInputStream fis = new FileInputStream(ff);
				BufferedInputStream bis = new BufferedInputStream(fis, 1024 * 1024);
				InputStreamReader isr = new InputStreamReader(bis, "UTF-8");
				BufferedReader in = new BufferedReader(isr);
				String l = null;
				while ((l = in.readLine()) != null) {
					Matcher m = hyphenWord.matcher(l);
					while (m.find()) {
						String w = m.group(1);
						w = w.toLowerCase();
						if (!w.equals("") && !w.equals("-") && !w.equals("--") && !w.startsWith("-") && !w.endsWith("-") && !w.equals("---")) {
							//System.out.println(w);
							words.add(w);
						}
					}
				}
				in.close();
				isr.close();
				bis.close();
				fis.close();
				c ++;
				if (c > 113) {
					break;
				}
			}
		}
		
		log.info("Sorting...");
		Map<String, Integer> sorted = sortByValue(words, true);
		
		log.info("Saving...");
		int threshold = 200;
		FileOutputStream fos2 = new FileOutputStream("hyphenated-alias.txt");
		OutputStreamWriter osw2 = new OutputStreamWriter(fos2, "UTF-8");
		FileOutputStream fos = new FileOutputStream("hyphenated-words.txt");
		OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
		for (Iterator<Entry<String, Integer>> it = sorted.entrySet().iterator(); it.hasNext();) {
			Entry<String, Integer> e = it.next();
			Integer count = e.getValue();
			String word = e.getKey();
			osw.write(String.valueOf(count));
			osw.write("\t");
			osw.write(word);
			osw.write("\n");
			
			if (count > threshold) {
				osw2.write(word);
				osw2.write("\n");
			}
		}
		osw.close();
		fos.close();
		osw2.close();
		fos2.close();
	}
	
	@Test
	public void testTokenizingHyphenatingAndStemming() throws IOException {
		Set<String> aliases = readHyphenatedAliases();
		System.out.println(tokenize("The black frog jumped over nice start-up and start-ups", aliases));
		System.out.println(tokenize("the startups and startup and start-up and start-ups and email and e-mail and e-mails is just the same words over and over", aliases));
	}

}
