package storm.cookbook.tfidf.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import storm.cookbook.tfidf.Conf;

public class TweetURLSpout extends BaseRichSpout {

	private Jedis jedis;
	private String host;
	private int port;
	private SpoutOutputCollector collector;

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("url"));
	}

	public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		host = conf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(conf.get(Conf.REDIS_PORT_KEY).toString());
		this.collector = spoutOutputCollector;
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
	}

	public void nextTuple() {
		String url = jedis.rpop("url");
		if (url == null) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}
		} else {
			collector.emit(new Values(url));
		}
	}

}
