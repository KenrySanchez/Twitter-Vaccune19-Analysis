package com.twitter.analysis;

import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Topology {

  static final String TOPOLOGY_NAME = "twitter-vaccune19-score-analysis";

  public static void main(String[] args) throws IOException {

    // Set config message.
    Config config = new Config();
    
    config.setMessageTimeoutSecs(180);

    // Build Storm Topology
    TopologyBuilder b = new TopologyBuilder();

    b.setSpout("TwitterSpout", new TwitterSpout());

    b.setBolt("FilterTweetsBolt", new FilterTweetsBolt()).shuffleGrouping("TwitterSpout");

    b.setBolt("PositiveTweetFilterBolt", new PositiveTweetFilterBolt())
        .allGrouping("FilterTweetsBolt");

    b.setBolt("NegativeTweetFilterBolt", new NegativeTweetFilterBolt())
        .allGrouping("FilterTweetsBolt");

    b.setBolt("ScoreSentimentalBolt", new ScoreSentimentBolt())
        .fieldsGrouping("PositiveTweetFilterBolt", new Fields(Utilities.TWITTER_ID_FIELD))
        .fieldsGrouping("NegativeTweetFilterBolt", new Fields(Utilities.TWITTER_ID_FIELD));

    //Init cluster as standalone
    final LocalCluster cluster = new LocalCluster();
    
    // Submit Topology
    cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

    // Shutdown cluster when kill Executor-Task
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
      }
    });

  }

}
