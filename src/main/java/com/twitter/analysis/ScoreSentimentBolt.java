package com.twitter.analysis;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ScoreSentimentBolt extends BaseRichBolt {

  /**
   * PRIVATE ATTRIBUTES
   */
  
  private static final long serialVersionUID = 1L;
  
  /**
   * BOLT OVERRIDE METHODS
   */

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void execute(Tuple input) {
    
    String text = (String) input.getValueByField(Utilities.TWITTER_ID_FIELD);
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // TODO Auto-generated method stub
    
  }

}
