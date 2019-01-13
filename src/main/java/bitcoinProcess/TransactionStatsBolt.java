package bitcoinProcess;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import scala.util.parsing.json.JSONObject;

public class TransactionStatsBolt extends BaseWindowedBolt {
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}
	@Override
	public void execute(TupleWindow inputWindow) {
		Integer tupleCount = 0;
		Double transaction_total_amount=0.;
		String now = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		for(Tuple input : inputWindow.get()) {
			
			transaction_total_amount += input.getDoubleByField("transaction_total_amount");
			//String transaction_hash = input.getStringByField("transaction_hash");
			//transaction_timestamp=input.getLongByField("transaction_timestamp");
			outputCollector.ack(input);
			tupleCount += 1;
		}
		
		System.out.printf("====== TransactionStatsBolt: Received %d tuples for %f bitcoins\n", tupleCount, transaction_total_amount);
		outputCollector.emit(new Values(transaction_total_amount,tupleCount,now));

	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transaction_total_amount","nb_transactions","date"));
	}

}
