package storm.topology;

import kafka.properties.KafkaProperties;
import storm.Scheme.MessageScheme;
import storm.bolt.AreaAmtBolt;
import storm.bolt.AreaFilterBolt;
import storm.bolt.ResultBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class AreaAmtTopo {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		BrokerHosts zkHosts = new ZkHosts(KafkaProperties.zkConnect);
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts,
				KafkaProperties.topic, "/storm-project", "KafkaSpout");
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

		builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig), 5);

		builder.setBolt("AreaFilterBolt", new AreaFilterBolt(), 5)
				.shuffleGrouping("KafkaSpout");

		builder.setBolt("AreaAmtBolt", new AreaAmtBolt(), 2).fieldsGrouping(
				"AreaFilterBolt", new Fields("area_id"));

		builder.setBolt("ResultBole", new ResultBolt(), 1).shuffleGrouping(
				"AreaAmtBolt");

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(2);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {

			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf,
					builder.createTopology());
		}

	}

}
