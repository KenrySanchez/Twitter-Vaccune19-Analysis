package com.twitter.analysis;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

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

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
