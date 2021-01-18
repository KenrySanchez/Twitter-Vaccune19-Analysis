package com.twitter.analysis;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Topology {

  static final String TOPOLOGY_NAME = "twitter-vaccune19-analysis";

  public static void main(String[] args) throws IOException {

    // Set config message. TODO: Look for improviment
    Config config = new Config();
    config.setMessageTimeoutSecs(120);

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
