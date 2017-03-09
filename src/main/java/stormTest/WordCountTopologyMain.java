package stormTest;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountTopologyMain {
	public static void main(String[] args) throws InterruptedException {  
        //定义一个Topology  
        TopologyBuilder builder = new TopologyBuilder();  
        builder.setSpout("word-reader",new WordReader(),1);  
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");  
        builder.setBolt("word-counter", new WordCounter(),2).fieldsGrouping("word-normalizer", new Fields("word"));  
        //配置  
        Config conf = new Config();  
        conf.put("wordsFile", "D:/words.txt");  
        conf.setDebug(false);  
        //提交Topology  
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);  
        //创建一个本地模式cluster  
        LocalCluster cluster = new LocalCluster();  
        cluster.submitTopology("Getting-Started-Toplogie", conf,builder.createTopology());  
        Utils.sleep(3000);  
        cluster.killTopology("Getting-Started-Toplogie");  
        cluster.shutdown();  
    }  
}
