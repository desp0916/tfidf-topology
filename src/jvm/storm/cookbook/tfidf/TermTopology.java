package storm.cookbook.tfidf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.cookbook.tfidf.bolt.PublishURLBolt;
import storm.cookbook.tfidf.functions.BatchCombiner;
import storm.cookbook.tfidf.functions.DocumentFetchFunction;
import storm.cookbook.tfidf.functions.DocumentTokenizer;
import storm.cookbook.tfidf.functions.PersistDocumentFunction;
import storm.cookbook.tfidf.functions.SplitAndProjectToFields;
import storm.cookbook.tfidf.functions.TermFilter;
import storm.cookbook.tfidf.functions.TfidfExpression;
import storm.cookbook.tfidf.spout.TwitterSpout;
import storm.cookbook.tfidf.state.TimeBasedRowStrategy;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.spout.ITridentSpout;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import trident.cassandra.CassandraState;

public class TermTopology {

	static Logger LOG = LoggerFactory.getLogger(TermTopology.class);

	private static String[] searchTerms = new String[] { "AAPL", "Mac", "iPhone", "iStore", "Apple" };
	private static String[] mimeTypes = new String[] { "application/pdf", "text/html", "text/plain" };

	private static StateFactory getStateFactory(String rowKey) {
		CassandraBucketState.BucketOptions options = new CassandraBucketState.BucketOptions();
		options.keyspace = "storm";
		options.columnFamily = "tfidf";
		options.rowKey = rowKey;
		options.keyStrategy = new TimeBasedRowStrategy();
		return CassandraBucketState.nonTransactional("localhost", options);
	}

	private static StateFactory getBatchStateFactory(String rowKey) {
		CassandraState.Options options = new CassandraState.Options();
		options.keyspace = "storm";
		options.columnFamily = "tfidfbatch";
		options.rowKey = rowKey;
		return CassandraState.nonTransactional("localhost", options);
	}

	public static StormTopology getTwitterTopology() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitterSpout", new TwitterSpout(searchTerms, 1000), 1);

		builder.setBolt("publishBolt", new PublishURLBolt(), 2).shuffleGrouping("twitterSpout");

		return builder.createTopology();
	}

	public static class PrintlnFunction extends BaseFunction {
		// @Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			System.out.println("New Tuple for printing: " + tuple.toString());
			collector.emit(new Values("dummy"));
		}
	}

	public static class StaticSourceFunction extends BaseFunction {
		private String source;

		public StaticSourceFunction(String source) {
			this.source = source;
		}

		// @Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			LOG.debug("Emitting static value");
			collector.emit(new Values(source));
		}
	}

	public static Stream getUrlStream(TridentTopology topology, ITridentSpout spout) {
		Stream urlStream = null;
		if (spout == null) {
			FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("url"), 1, new Values("doc01"),
					new Values("doc02"), new Values("doc03"), new Values("doc04"), new Values("doc05"));
			testSpout.setCycle(true);
			urlStream = topology.newStream("spout1", testSpout);
		} else {
			urlStream = topology.newStream("spout1", spout);
		}

		return urlStream.parallelismHint(16);
	}

	public static void addDQueryStream(TridentState state, TridentTopology topology, LocalDRPC drpc) {
		topology.newDRPCStream("dQuery", drpc).each(new Fields("args"), new Split(), new Fields("source"))
				.stateQuery(state, new Fields("source"), new MapGet(), new Fields("d"))
				.each(new Fields("d"), new FilterNull()).project(new Fields("source", "d"));
	}

	private static void addDFQueryStream(TridentState dfState, TridentTopology topology, LocalDRPC drpc) {
		topology.newDRPCStream("dfQuery", drpc).each(new Fields("args"), new Split(), new Fields("term"))
				.stateQuery(dfState, new Fields("term"), new MapGet(), new Fields("df"))
				.each(new Fields("df"), new FilterNull()).project(new Fields("term", "df"));
	}

	private static void addTFIDFQueryStream(TridentState tfState, TridentState dfState, TridentState dState,
			TridentTopology topology, LocalDRPC drpc) {
		TridentState batchDfState = topology.newStaticState(getBatchStateFactory("df"));
		TridentState batchDState = topology.newStaticState(getBatchStateFactory("d"));
		TridentState batchTfState = topology.newStaticState(getBatchStateFactory("tf"));

		topology.newDRPCStream("tfidfQuery", drpc)
				.each(new Fields("args"), new SplitAndProjectToFields(), new Fields("documentId", "term"))
				.each(new Fields(), new StaticSourceFunction("twitter"), new Fields("source"))
				.stateQuery(tfState, new Fields("documentId", "term"), new MapGet(), new Fields("tf_rt"))
				.stateQuery(dfState, new Fields("term"), new MapGet(), new Fields("df_rt"))
				.stateQuery(dState, new Fields("source"), new MapGet(), new Fields("d_rt"))
				.stateQuery(batchTfState, new Fields("documentId", "term"), new MapGet(), new Fields("tf_batch"))
				.stateQuery(batchDfState, new Fields("term"), new MapGet(), new Fields("df_batch"))
				.stateQuery(batchDState, new Fields("source"), new MapGet(), new Fields("d_batch"))
				.each(new Fields("tf_rt", "df_rt", "d_rt", "tf_batch", "df_batch", "d_batch"), new BatchCombiner(),
						new Fields("tf", "d", "df"))
				.each(new Fields("term", "documentId", "tf", "d", "df"), new TfidfExpression(), new Fields("tfidf"))
				.each(new Fields("tfidf"), new FilterNull()).project(new Fields("documentId", "term", "tfidf"));
	}

	@SuppressWarnings("rawtypes")
	public static TridentTopology buildTopology(ITridentSpout spout, LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();

		Stream documentStream = getUrlStream(topology, spout).each(new Fields("url"),
				new DocumentFetchFunction(mimeTypes), new Fields("document", "documentId", "source"));

		documentStream.each(new Fields("documentId", "document"), new PersistDocumentFunction(), new Fields());

		Stream termStream = documentStream.parallelismHint(20)
				.each(new Fields("document"), new DocumentTokenizer(), new Fields("dirtyTerm"))
				.each(new Fields("dirtyTerm"), new TermFilter(), new Fields("term"))
				.project(new Fields("term", "documentId", "source"));

		TridentState dfState = termStream.groupBy(new Fields("term")).persistentAggregate(getStateFactory("df"),
				new Count(), new Fields("df"));

		addDFQueryStream(dfState, topology, drpc);

		TridentState dState = documentStream.groupBy(new Fields("source")).persistentAggregate(getStateFactory("d"),
				new Count(), new Fields("d"));

		addDQueryStream(dState, topology, drpc);

		TridentState tfState = termStream.groupBy(new Fields("documentId", "term"))
				.persistentAggregate(getStateFactory("tf"), new Count(), new Fields("tf"));

		addTFIDFQueryStream(tfState, dfState, dState, topology, drpc);

		return topology;
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		conf.put(Conf.REDIS_HOST_KEY, "localhost");
		conf.put(Conf.REDIS_PORT_KEY, Conf.DEFAULT_JEDIS_PORT);
		conf.put("DOCUMENT_PATH", "document.avro");
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			conf.setDebug(true);
			TridentTopology topology = buildTopology(null, drpc);
			cluster.submitTopology("tfidf", conf, topology.build());
			for (int i = 0; i < 100; i++) {
				System.out.println("About to query!");
				System.out.println("DRPC RESULT: " + drpc.execute("tfidfQuery", "doc01 area"));
				Thread.sleep(1000);
			}
		} else {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology("twitter", conf, getTwitterTopology());
			// TODO: Create the twitter spout and pass it in here...
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null, null).build());
		}
	}

}
