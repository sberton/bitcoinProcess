package bitcoinProcess;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SaveResultsBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	
	@Override
	public void execute(Tuple input) {
		try {
			process(input);
			outputCollector.ack(input);
		} catch (IOException e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}
	}
	
	public void process(Tuple input) throws IOException {
		Double transaction_total_amount = input.getDoubleByField("transaction_total_amount");
		Integer nb_transactions = input.getIntegerByField("nb_transactions");
		String date = input.getStringByField("date");
		String filePath = "/data/result/result.csv";
		
		// Check if file exists
		File csvFile = new File(filePath);
		if(!csvFile.exists()) {
			FileWriter fileWriter = new FileWriter(filePath);
			fileWriter.write("nb_transactions;transaction_total_amount;transaction_timestamp\n");
			fileWriter.close();
		}
		
		// Write stats to file
		FileWriter fileWriter = new FileWriter(filePath, true);
		System.out.printf("====== SaveResultsBolt: transaction_total_amount %f\n", transaction_total_amount);
		fileWriter.write(String.format("%d;%f;%s\n", nb_transactions,transaction_total_amount,date));
		fileWriter.close();
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
