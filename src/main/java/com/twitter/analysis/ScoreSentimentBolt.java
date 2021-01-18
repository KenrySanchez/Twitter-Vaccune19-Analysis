package com.twitter.analysis;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private Map<String, Integer> sentimentScoreCounter;

  private static final Logger logger = LoggerFactory.getLogger(ScoreSentimentBolt.class);

  /**
   * BOLT OVERRIDE METHODS
   */

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    sentimentScoreCounter = new HashMap<String, Integer>();
  }

  @Override
  public void execute(Tuple input) {

    String id = (String) input.getValueByField(Utilities.TWITTER_ID_FIELD);
    Integer score = (Integer) input.getValueByField(Utilities.TWITTER_SCORE_FIELD);

    Integer sentimentScore = sentimentScoreCounter.get(id);

    if (sentimentScore == null) {

      sentimentScore = score;
      sentimentScoreCounter.put(id, sentimentScore);

    } else {

      final int finalScore = (sentimentScore + score);

      logger.info(new StringBuilder("mention - ").append("----NA-----").append(" :: score - ")
          .append(finalScore).toString());

    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // TODO Auto-generated method stub

  }

}
