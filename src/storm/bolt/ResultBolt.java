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
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class ResultBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -8367466490648860751L;
	Map<String, Double> countMap = null;
	HbaseDao hbaseDao = null;
	String today = "";
	long beginTime = 0L;
	long endTime = 0L;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context) {
		super.prepare(stormConf, context);
		today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		countMap = new HashMap<String, Double>();
		hbaseDao = new HbaseDaoImpl();
		beginTime = System.currentTimeMillis();
		endTime = 0L;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String date_areaid = tuple.getString(0);
		Double order_amt = tuple.getDouble(1);

		/* 保存昨天的数据，并清空map */
		String[] tem = StringUtils.split(date_areaid, "_");
		if (!StringUtils.equals(tem[0], today)) {
			Iterator<Entry<String, Double>> iterator = countMap.entrySet()
					.iterator();
			boolean changToToday = true;
			List<String> toDel = new ArrayList<String>();
			while (iterator.hasNext()) {
				Entry<String, Double> next = iterator.next();
				String[] tem1 = StringUtils.split(next.getKey(), "_");
				if (!StringUtils.equals(tem1[0], tem[0])) {
					changToToday = false;
					if (StringUtils.equals(tem1[1], tem[1])) {
						hbaseDao.save("area_order", next.getKey(), "cf",
								"order_amt", Bytes.toBytes(next.getValue()));
						toDel.add(next.getKey());
					}

				}

			}

			for (String str : toDel) {
				countMap.remove(str);
			}
			if (changToToday) {
				today = tem[0];
			}

		}

		countMap.put(date_areaid, order_amt);

		endTime = System.currentTimeMillis();
		if (endTime - beginTime >= 5 * 1000) {
			Iterator<Entry<String, Double>> iterator = countMap.entrySet()
					.iterator();
			while (iterator.hasNext()) {
				Entry<String, Double> next = iterator.next();

				hbaseDao.save("area_order", next.getKey(), "cf", "order_amt",
						Bytes.toBytes(next.getValue()));
			}
			beginTime = System.currentTimeMillis();
		}
	}

	@Override
	public void cleanup() {
		super.cleanup();
		Iterator<Entry<String, Double>> iterator = countMap.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Entry<String, Double> next = iterator.next();

			hbaseDao.save("area_order", next.getKey(), "cf", "order_amt",
					Bytes.toBytes(next.getValue()));
		}

		countMap.clear();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
