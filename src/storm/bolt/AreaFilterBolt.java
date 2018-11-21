package storm.bolt;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AreaFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 4589207431728338405L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String order = tuple.getString(0);
		if (StringUtils.isNotEmpty(order)) {
			String[] orderArr = StringUtils.split(order, "\t");
			// area_id order_amt create_time
			collector.emit(new Values(orderArr[3], orderArr[1], orderArr[2]));
		
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("area_id", "order_amt", "order_date"));

	}

}
