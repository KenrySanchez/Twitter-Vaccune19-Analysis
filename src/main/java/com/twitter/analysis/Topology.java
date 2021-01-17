package com.twitter.analysis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	static final String TOPOLOGY_NAME = "twitter-vaccune19-analysis";

	public static void main(String[] args) {

		//Set config message. TODO: Look for improviment
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		//Build Storm Topology
		TopologyBuilder b = new TopologyBuilder();

		b.setSpout("TwitterSpout", new TwitterSpout());

		//TODO: add the list of irrelevant words
		b.setBolt("FilterTweetsBolt", new FilterTweetsBolt()).shuffleGrouping("TwitterSpout");
				
		b.setBolt("PositiveTweetFilterBolt", new PositiveTweetFilterBolt()).shuffleGrouping("FilterTweetsBolt");
		
		//TODO: cambiar
//		b.setBolt("SentimentAnalysisBolt", new SentimentAnalysisBolt(10, 10 * 60))
//				.shuffleGrouping("TweetWordSplitterBolt");

		//Defining cluster configuration (TODO: Optimize if neccesary)
		final LocalCluster cluster = new LocalCluster();

		//Submit Topology
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		//Shutdown cluster when kill Executor-Task
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
