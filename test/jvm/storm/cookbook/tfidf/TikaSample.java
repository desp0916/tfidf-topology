package storm.cookbook.tfidf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.tartarus.snowball.ext.PorterStemmer;
import org.xml.sax.ContentHandler;

public class TikaSample {

	static PorterStemmer stemmer = new PorterStemmer();

	public static void main(String[] args) throws Exception {
		// parse out the directory that we want to crawl
		if (args.length != 1) {
			showUsageAndExit();
		}

		File directory = new File(args[0]);
		if (!directory.isDirectory()) {
			showUsageAndExit();
		}

		parseAllFilesInDirectory(directory);
	}

	private static void parseAllFilesInDirectory(File directory) throws Exception {
		for (File file : directory.listFiles()) {
			if (file.isDirectory()) {
				parseAllFilesInDirectory(file);
			} else {
				Parser parser = new AutoDetectParser();

				Metadata metadata = new Metadata();
				ParseContext parseContext = new ParseContext();

				ContentHandler handler = new BodyContentHandler(10 * 1024 * 1024);
				parser.parse(new FileInputStream(file), handler, metadata, parseContext);

				System.out.println("-------------------------------------------------------");
				System.out.println("File: " + file);
				for (String name : metadata.names()) {
					System.out.println("metadata: " + name + " - " + metadata.get(name));
				}
				if (metadata.get("Content-Type").contains("pdf")) {
					// printTerms(handler.toString());
					for (String name : metadata.names()) {
						System.out.println("metadata: " + name + " - " + metadata.get(name));
					}
					// System.out.println("Content: " + handler.toString());
				}
			}
		}
	}

	private static void printTerms(String documentContents) {
		try {
			TokenStream ts = new StopFilter(Version.LUCENE_30,
					new StandardTokenizer(Version.LUCENE_30, new StringReader(documentContents)),
					StopAnalyzer.ENGLISH_STOP_WORDS_SET);
			TermAttribute termAtt = ts.getAttribute(TermAttribute.class);
			ts.getAttribute(TermAttribute.class);
			while (ts.incrementToken()) {
				stemmer.setCurrent(termAtt.term().toLowerCase());
				stemmer.stem();
				System.out.println(stemmer.getCurrent());
			}
			ts.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void showUsageAndExit() {
		System.err.println("Usage: java TikaSample <directory to crawl>");
		System.exit(1);
	}
}
