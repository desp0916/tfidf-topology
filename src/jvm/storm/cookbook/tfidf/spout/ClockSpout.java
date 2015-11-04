package storm.cookbook.tfidf.spout;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

/**
 * A generic template for a Spout that emits a tuple at a regular interval (in
 * terms of real time).
 *
 * Due to the time-sensitive nature of this task, there is no point in making
 * these ticks reliable.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public abstract class ClockSpout implements IBatchSpout {
	private SpoutOutputCollector collector;

	protected final String streamId;
	private int i;

	/**
	 * @param streamId
	 *            The stream on which to emit
	 */
	protected ClockSpout(String streamId) {
		this.streamId = streamId;
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		i = 0;
	}

	public void close() {
	}

	public void emitBatch(long batchId, TridentCollector collector) {
		collector.emit(getTupleForTick(i));
		Utils.sleep(getDelayForTick(i));

		i++;
	}

	/**
	 * Returns the data to be included in the {@code i}-th tick. Subclasses
	 * should ensure that they are also overriding
	 * {@link #declareOutputFields(OutputFieldsDeclarer) declareOutputFields} to
	 * match the structure being returned here.
	 *
	 * @param i
	 *            the number of ticks that have already been emitted by this
	 *            spout
	 * @return the data to be included in the {@code i}-th tick
	 */
	public abstract List<Object> getTupleForTick(int i);

	/**
	 * Returns the delay between the {@code i} and {@code i+1}-th ticks, in
	 * milliseconds.
	 *
	 * @param i
	 *            the number of ticks that have already been emitted by this
	 *            spout
	 * @return the delay between the {@code i} and {@code i+1}-th ticks
	 */
	public abstract long getDelayForTick(int i);

	/**
	 * Marked final to emphasize to subclasses that there is no point in
	 * implementing this (emitted tuples are not anchored).
	 */
	@Override
	public final void ack(long batchId) {
	}

}
