package com.twitter.analysis;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PositiveTweetFilterBolt extends BaseRichBolt {

  /**
   * PRIVATE ATTRIBUTES
   */

  private static final long serialVersionUID = 1L;

  private OutputCollector collector;

  /**
   * BOLT OVERRIDE METHODS
   */

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {

    String text = (String) input.getValueByField(Utilities.TWITTER_TEXT_FIELD);
    String id = (String) input.getValueByField(Utilities.TWITTER_ID_FIELD);

    String original = (String) input.getValueByField(Utilities.TWITTER_ORIGINAL_FIELD);

    int sentimentScore = 0;

    String[] words = text.split(Utilities.BLANK_SPACE_DELIMITED);
    for (String word : words) {

      if (Utilities.POSITIVE_WORDS.contains(word)) {
        sentimentScore += 1;
      }

    }

    collector.emit(new Values(id, sentimentScore, original));

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Utilities.TWITTER_ID_FIELD, Utilities.TWITTER_SCORE_FIELD,
        Utilities.TWITTER_ORIGINAL_FIELD));
  }

}
