package stormDemo;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SenqueceBolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String word = (String) input.getValue(0);  
        String out = "I'm " + word +  "!";  
        System.out.println("out=" + out);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
