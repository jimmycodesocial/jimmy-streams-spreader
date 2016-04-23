/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.apache.log4j.Logger;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

import java.util.Map;

/**
 * Bolt that listen for audiences and retrieve the list of streams subscribed to the audience.
 * This bolt will emit a copy of the activity per each stream subscribed.
 */
public class SubscriptionsBolt extends BaseBasicBolt {
    /**
     * Collector to ACK tuples.
     */
    private OutputCollector _collector;

    /**
     * Logger instance.
     */
    final static Logger logger = Logger.getLogger(SubscriptionsBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stream", "activity"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context);
        this._collector = collector;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // {"id": "<id>", "name": "<name>"}
        JSONObject audience = (JSONObject)input.getValueByField("audience");
        JSONObject activity = (JSONObject)input.getValueByField("activity");

        // TODO: Don't emit the same audience as stream, instead of that, search all subscriptions.
        this._collector.emit(new Values(audience, activity));
        this._collector.ack(input);
    }
}
