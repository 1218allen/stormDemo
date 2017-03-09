package stormTest;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounter implements IRichBolt {
	Integer id;  
    String name;  
    Map<String, Integer> counters;  
    private OutputCollector collector;  
  
    public void prepare(Map stormConf, TopologyContext context,  
            OutputCollector collector) {  
        this.counters = new HashMap<String, Integer>();  
        this.collector = collector;  
        this.name = context.getThisComponentId();  
        this.id = context.getThisTaskId();  
  
    }  
    public void execute(Tuple input) {  
        String str = input.getString(0);  
        if (!counters.containsKey(str)) {  
            counters.put(str, 1);  
        } else {  
            Integer c = counters.get(str) + 1;  
            counters.put(str, c);  
        }  
        // ȷ�ϳɹ�����һ��tuple  
        collector.ack(input);  
    }  
    /** 
     * Topologyִ����ϵ�������������ر����ӡ��ͷ���Դ�Ȳ�������д������ 
     * ��Ϊ��ֻ�Ǹ�Demo��������������ӡ���ǵļ����� 
     * */  
    public void cleanup() {  
        System.out.println("-- Word Counter [" + name + "-" + id + "] --");  
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {  
            System.out.println(entry.getKey() + ": " + entry.getValue());  
        }  
        counters.clear();  
    }  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        // TODO Auto-generated method stub  
  
    }  
    public Map<String, Object> getComponentConfiguration() {  
        // TODO Auto-generated method stub  
        return null;  
    }
}
