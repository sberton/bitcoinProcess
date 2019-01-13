package bitcoinProcess;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
public class App 
{
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException 
    {
    	TopologyBuilder builder = new TopologyBuilder();
    	KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("localhost:19092,localhost:19093", "bitcoin-transaction");
    	spoutConfigBuilder.setGroupId("transactions-stats");
    	
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	builder.setSpout("transaction", new KafkaSpout<String, String>(spoutConfig));
    	builder.setBolt("transaction-parsing", new TransactionParser(),2).shuffleGrouping("transaction");
    	builder.setBolt("transaction-stats", new TransactionStatsBolt().withWindow(BaseWindowedBolt.Duration.minutes(1),BaseWindowedBolt.Duration.seconds(30)))
    	.shuffleGrouping("transaction-parsing");
    	builder.setBolt("save-results",  new SaveResultsBolt())
		.shuffleGrouping("transaction-stats");
    	
    	StormTopology topology = builder.createTopology();

    	Config config = new Config();
    	/*timeout had to be greater than sliding windows duration*/
		config.setMessageTimeoutSecs(4000);
    	String topologyName = "bitcoin-processor";
    	if(args.length > 0 && args[0].equals("remote")) {
    		StormSubmitter.submitTopology(topologyName, config, topology);
    	}
    	else {
    		LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology(topologyName, config, topology);
    	}
    }
}
