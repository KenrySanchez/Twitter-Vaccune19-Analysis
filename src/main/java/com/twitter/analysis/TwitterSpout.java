package com.twitter.analysis;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterSpout extends BaseRichSpout {

	/**
	 * PRIVATE ATTRIBUTES
	 */

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;

	/**
	 * SPOUT OVERRIDE METHODS
	 */

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		//Twitter Listener Stream API
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onStallWarning(StallWarning stallWarning) {
			}

			@Override
			public void onException(Exception e) {
			}
		};

		// Twitter Stream configuration
		TwitterStreamFactory factory = new TwitterStreamFactory();
		
		twitterStream = factory.getInstance();
		
		FilterQuery query = new FilterQuery();
		
		//Query by hashtags and Language
		query.track(new String[]{ "#COVID19", "#Vaccine", "#COVIDVaccine" });
		query.language(new String[] {"en"});
		
		//load listener and start filtering
		twitterStream.addListener(listener);
		twitterStream.filter(query);
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();

		if (ret == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Utilities.TWITTER_LIST_FIELD));
	}

}
