package stormTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer implements IRichBolt {

	private OutputCollector collector;  
    public void prepare(Map stormConf, TopologyContext context,  
            OutputCollector collector) {  
        this.collector = collector;  
    }  
    /**����bolt������Ҫ�ķ�����ÿ�����յ�һ��tupleʱ���˷����㱻���� 
     * ������������þ��ǰ��ı��ļ��е�ÿһ���зֳ�һ�������ʣ�������Щ���ʷ����ȥ������һ��bolt���� 
     * **/  
    public void execute(Tuple input) {  
        String sentence = input.getString(0);  
        String[] words = sentence.split(" ");  
        for (String word : words) {  
            word = word.trim();  
            if (!word.isEmpty()) {  
                word = word.toLowerCase();  
                // Emit the word  
                List a = new ArrayList();  
                a.add(input);  
                collector.emit(a, new Values(word));  
            }  
        }  
        //ȷ�ϳɹ�����һ��tuple  
        collector.ack(input);  
    }  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        declarer.declare(new Fields("word"));  
  
    }  
    public void cleanup() {  
        // TODO Auto-generated method stub  
  
    }  
    public Map<String, Object> getComponentConfiguration() {  
        // TODO Auto-generated method stub  
        return null;  
    } 

}
