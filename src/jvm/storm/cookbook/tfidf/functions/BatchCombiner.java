package storm.cookbook.tfidf.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class BatchCombiner extends BaseFunction {

	Logger LOG = LoggerFactory.getLogger(BatchCombiner.class);
	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			double d_rt = tuple.getLongByField("d_rt");
			double df_rt = tuple.getLongByField("df_rt");
			double tf_rt = tuple.getLongByField("tf_rt");

			double d_batch = tuple.getLongByField("d_batch");
			double df_batch = tuple.getLongByField("df_batch");
			double tf_batch = tuple.getLongByField("tf_batch");

			LOG.debug("Combining! d_rt=" + d_rt + "df_rt=" + df_rt + "tf_rt=" + tf_rt + "d_batch=" + d_batch
					+ "df_batch=" + df_batch + "tf_batch=" + tf_batch);

			collector.emit(new Values(tf_rt + tf_batch, d_rt + d_batch, df_rt + df_batch));
		} catch (Exception e) {
		}

	}

}
