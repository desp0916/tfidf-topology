package storm.cookbook.tfidf.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import twitter4j.Status;
import twitter4j.URLEntity;

public class PublishURLBolt extends BaseRichBolt {

	Jedis jedis;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		jedis = new Jedis("localhost");
	}

	public void execute(Tuple input) {
		Status ret = (Status) input.getValue(0);
		URLEntity[] urls = ret.getURLEntities();
		for (int i = 0; i < urls.length; i++) {
			jedis.rpush("url", urls[i].getURL().trim());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
