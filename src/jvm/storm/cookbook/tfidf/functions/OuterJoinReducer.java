package storm.cookbook.tfidf.functions;

import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.MultiReducer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.tuple.TridentTuple;

public class OuterJoinReducer implements MultiReducer<OuterJoinState> {
	private static final long serialVersionUID = 1L;

	public void prepare(Map conf, TridentMultiReducerContext context) {

	}

	public OuterJoinState init(TridentCollector collector) {
		return new OuterJoinState();
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void execute(OuterJoinState state, int streamIndex, TridentTuple input, TridentCollector collector) {
		state.addValues(streamIndex, input);
	}

	public void complete(OuterJoinState state, TridentCollector collector) {
		for (Values vals : state.join()) {
			collector.emit(vals);
		}
	}

}
