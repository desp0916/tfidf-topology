package storm.cookbook.tfidf.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TfidfExpression extends BaseFunction {

	Logger LOG = LoggerFactory.getLogger(TfidfExpression.class);
	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			double d = tuple.getLongByField("d");
			double df = tuple.getLongByField("df");
			double tf = tuple.getLongByField("tf");
			LOG.debug("d=" + d + "df=" + df + "tf=" + tf);
			double tfidf = tf * Math.log(d / (1 + df));
			LOG.debug("Emitting new TFIDF(term,Document): (" + tuple.getStringByField("term") + ","
					+ tuple.getStringByField("documentId") + ") = " + tfidf);
			collector.emit(new Values(tfidf));
		} catch (Exception e) {
		}

	}

}
