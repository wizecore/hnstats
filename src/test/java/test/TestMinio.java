package test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.HashMap;

import org.junit.After;
import org.junit.Test;
import org.xmlpull.v1.XmlPullParserException;

import io.minio.MinioClient;
import io.minio.Result;
import io.minio.errors.MinioException;
import io.minio.messages.Item;

public class TestMinio extends BaseUtil {

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws MinioException, GeneralSecurityException, IOException, XmlPullParserException {
		MinioClient mc = new MinioClient(
			env("AWS_S3_ENDPOINT", "https://s3.amazonaws.com"), 
			env("AWS_ACCESS_KEY_ID", ""), 
			env("AWS_SECRET_ACCESS_KEY", ""), 
			env("AWS_DEFAULT_REGION", "us-east-1")
		);
		//mc.setTimeout(0, 0, 0);
		String bucket = env("AWS_S3_DEFAULT_BUCKET", "test");
		Iterable<Result<Item>> it = mc.listObjects(bucket);
		HashMap<String, Item> found = new HashMap<>();
		for (Result<Item> r: it) {
			log.info("Found: " + r.get().get("Key"));
			found.put((String) r.get().get("Key"), r.get());
		}
		
		File[] ff = new File(".").listFiles();
		for (File f: ff) {
			if (f.getName().startsWith("data-") && f.getName().endsWith(".tsv")) {
				log.info("Uploading " + f.getName());
				if (found.containsKey(f.getName())) {
					log.info("Removing existing " + f.getName());
					mc.removeObject(bucket, f.getName());
				}
				
				long size = f.length();
				long start = System.currentTimeMillis();
				InputStream is = new FileInputStream(f);
				BufferedInputStream bis = new BufferedInputStream(is, 1024 * 1024);
				mc.putObject(bucket, f.getName(), bis, size, "text/plain");
				bis.close();
				is.close();
				long end = System.currentTimeMillis();
				log.info("Uploaded " + (size/1024) + " kbytes, speed " + (size/(1024.0 * (end - start))) * 1000.0 + " kbps");
			}
		}
	}

	protected String env(String n, String def) {
		String v = System.getenv(n);
		if (v != null) {
			return v;
		} else {
			return def;
		}
	}

}
