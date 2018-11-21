package storm.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import myhbase.dao.HbaseDao;
import myhbase.dao.HbaseDaoImpl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AreaAmtBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -6841021712438315936L;

	Map<String, Double> countMap = null;
	HbaseDao hbaseDao = null;
	String today = "";

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		hbaseDao = new HbaseDaoImpl();
		// 根据hbase中保存的值进行初始化
		countMap = this.initMap(today, hbaseDao);

	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		String area_id = tuple.getString(0);
		double order_amt = Double.parseDouble(tuple.getString(1));
		String order_date = tuple.getString(2);

		if (!StringUtils.equals(order_date, today)) {
			Iterator<Entry<String, Double>> ito = countMap.entrySet()
					.iterator();
			boolean changToToday = true;
			List<String> toDel = new ArrayList<String>();
			while (ito.hasNext()) {// 遍历countMap
				Entry<String, Double> next = ito.next();// 获得的一条记录
				// 分隔，0为日期、1为区域id
				String[] tem = StringUtils.split(next.getKey(), "_");
				if (!StringUtils.equals(order_date, tem[0])) {
					changToToday = false;
					if (StringUtils.equals(tem[1], area_id)) {
						toDel.add(next.getKey());
					}
				}
			}

			for (String str : toDel) {
				countMap.remove(str);
			}

			// countMap = this.initMap(order_date, hbaseDao);
			if (changToToday) {
				today = order_date;
			}

		}

		Double count = countMap.get(order_date + "_" + area_id);
		if (null == count) {
			count = 0.0;
		}
		count += order_amt;
		countMap.put(order_date + "_" + area_id, count);
		collector.emit(new Values(order_date + "_" + area_id, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date_area", "amt"));
	}

	@Override
	public void cleanup() {
		super.cleanup();
		countMap.clear();
	}

	public Map<String, Double> initMap(String rowKeyDate, HbaseDao dao) {

		Map<String, Double> countMap = new HashMap<String, Double>();

		List<Result> rows = dao.getRows("area_order", rowKeyDate, "cf",
				"order_amt");

		for (Result rs : rows) {
			for (Cell cell : rs.listCells()) {
				countMap.put(Bytes.toString(CellUtil.cloneRow(cell)),
						Bytes.toDouble(CellUtil.cloneValue(cell)));
			}
		}

		return countMap;
	}

}
