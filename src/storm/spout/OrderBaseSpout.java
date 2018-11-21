package storm.spout;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.consumer.OrderConsumer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class OrderBaseSpout implements IRichSpout {

	private static final long serialVersionUID = -3118566615647740879L;
	private Queue<String> queue = new ConcurrentLinkedQueue<String>();

	Integer TaskId = null;
	SpoutOutputCollector collector = null;
	String topic = null;

	public OrderBaseSpout(String topic) {
		this.topic = topic;
	}

	@Override
	public void ack(Object arg0) {

	}

	@Override
	public void activate() {

	}

	@Override
	public void close() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void fail(Object arg0) {

	}

	@Override
	public void nextTuple() {
		if (queue.size() > 0) {
			String str = queue.poll();
			System.out.println("TaskId:" + TaskId + ";  str=" + str);
			collector.emit(new Values(str));
		}
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		TaskId = context.getThisTaskId();
		OrderConsumer consumer = new OrderConsumer(topic);
		consumer.start();
		queue = consumer.getQueue();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("order"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
