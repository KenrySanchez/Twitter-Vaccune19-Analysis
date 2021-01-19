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

    //get tweet properties
    String id = (String) input.getValueByField(Utilities.TWITTER_ID_FIELD);
    Integer score = (Integer) input.getValueByField(Utilities.TWITTER_SCORE_FIELD);
    String original = (String) input.getValueByField(Utilities.TWITTER_ORIGINAL_FIELD);

    //get sentimental count if exist
    Integer sentimentScore = sentimentScoreCounter.get(id);

    //define if the tweet is positive or negative
    if (sentimentScore == null) {

      sentimentScore = score;
      sentimentScoreCounter.put(id, sentimentScore);

    } else {

      final int finalScore = (sentimentScore + score);

      String finalWord = finalScore >= 1 ? "POSITIVE" : finalScore <= -1 ? "NEGATIVE" : "NA";

      //track logger. In distributed mode, log can be watch it at /var/log/storm/
      logger.info(new StringBuilder("TWEET: ").append(original).append("- " + finalWord)
          .append(" -- SCORE: ").append(finalScore).toString());
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // TODO Auto-generated method stub
  }

}
