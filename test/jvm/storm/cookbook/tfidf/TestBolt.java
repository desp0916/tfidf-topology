package storm.cookbook.tfidf;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

public final class TestBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private static final transient Logger LOG = Logger.getLogger(TestBolt.class);

	private static Jedis jedis;

	private String channel;

	public TestBolt(String channel) {
		this.channel = channel;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		jedis = new Jedis("localhost", Integer.parseInt(Conf.DEFAULT_JEDIS_PORT));
		jedis.connect();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public void execute(Tuple input) {
		jedis.rpush(channel, input.getString(1));

	}

}
