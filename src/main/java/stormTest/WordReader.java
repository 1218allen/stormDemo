package stormTest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader implements IRichSpout {

	private static final long serialVersionUID = 1L;  
    private SpoutOutputCollector collector;  
    private FileReader fileReader;  
    private boolean completed = false;  
  
    public boolean isDistributed() {  
        return false;  
    }  
    /** 
     * ���ǵ�һ�����������������������������һ���Ǵ���Topologyʱ�����ã� 
     * �ڶ��������е�Topology���ݣ���������������Spout�����ݷ����bolt 
     * **/  
    public void open(Map conf, TopologyContext context,  
            SpoutOutputCollector collector) {  
        try {  
            //��ȡ����Topologyʱָ����Ҫ��ȡ���ļ�·��  
            this.fileReader = new FileReader(conf.get("wordsFile").toString());  
        } catch (FileNotFoundException e) {  
            throw new RuntimeException("Error reading file ["  
                    + conf.get("wordFile") + "]");  
        }  
        //��ʼ��������  
        this.collector = collector;  
  
    }  
    /** 
     * ����Spout����Ҫ�ķ��������������Ƕ�ȡ�ı��ļ�����������ÿһ�з����ȥ����bolt�� 
     * ��������᲻�ϱ����ã�Ϊ�˽�������CPU�����ģ����������ʱ����sleepһ�� 
     * **/  
    public void nextTuple() {  
        if (completed) {  
            try {  
                Thread.sleep(1000);  
            } catch (InterruptedException e) {  
                // Do nothing  
            }  
            return;  
        }  
        String str;  
        // Open the reader  
        BufferedReader reader = new BufferedReader(fileReader);  
        try {  
            // Read all lines  
            while ((str = reader.readLine()) != null) {  
                /** 
                 * ����ÿһ�У�Values��һ��ArrayList��ʵ�� 
                 */  
                this.collector.emit(new Values(str), str);  
            }  
        } catch (Exception e) {  
            throw new RuntimeException("Error reading tuple", e);  
        } finally {  
            completed = true;  
        }  
  
    }  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        declarer.declare(new Fields("line"));  
  
    }  
    public void close() {  
        // TODO Auto-generated method stub  
    }  
      
    public void activate() {  
        // TODO Auto-generated method stub  
  
    }  
    public void deactivate() {  
        // TODO Auto-generated method stub  
  
    }  
    public void ack(Object msgId) {  
        System.out.println("OK:" + msgId);  
    }  
    public void fail(Object msgId) {  
        System.out.println("FAIL:" + msgId);  
  
    }  
    public Map<String, Object> getComponentConfiguration() {  
        // TODO Auto-generated method stub  
        return null;  
    }

}
