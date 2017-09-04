package test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

public class TestLemmatizer extends BaseUtil {

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws IOException {
		Set<String> aliases = readHyphenatedAliases();
		System.out.println(lemmatize("The black frog jumped over nice start-up and start-ups"));
		System.out.println(lemmatize(tokenize("the startups and startup and start-up and start-ups and email and e-mail and e-mails is just the same words over and over", aliases)));
	}

}
