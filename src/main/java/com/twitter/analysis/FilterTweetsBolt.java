package com.twitter.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class FilterTweetsBolt extends BaseRichBolt {

  /**
   * PRIVATE ATRIBUTES
   */

  private static final long serialVersionUID = 5151173513759399636L;

  private OutputCollector collector;

  /**
   * BOLT OVERRIDE METHODS
   */

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    Status tweet = (Status) input.getValueByField(Utilities.TWITTER_LIST_FIELD);

    String text = tweet.getText().replaceAll("\\p{Punct}", Utilities.BLANK_SPACE_DELIMITED)
        .replaceAll("\\r|\\n", "").toLowerCase();

    String originalText = tweet.getText();

    for (String word : Utilities.STOP_WORDS) {

      text = text.replaceAll("\\b" + word.toLowerCase() + "\\b", "");
    }

    collector.emit(new Values(String.valueOf(tweet.getId()), text, originalText));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Utilities.TWITTER_ID_FIELD, Utilities.TWITTER_TEXT_FIELD,
        Utilities.TWITTER_ORIGINAL_FIELD));
  }
}
