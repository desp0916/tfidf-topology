package storm.cookbook.tfidf;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestDocumentFetch {

	@Test
	public void test() {
		List<String> mimeTypes = Arrays.asList(new String[] { "application/pdf", "text/html", "text/plain" });
		assertTrue(mimeTypes.contains("text/html"));
	}

}
