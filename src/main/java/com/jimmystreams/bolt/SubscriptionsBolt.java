/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.apache.storm.tuple.Values;
import org.json.JSONObject;

import java.util.Map;

/**
 * Bolt that listen for audiences and retrieve the list of streams subscribed to the audience.
 * This bolt will emit a copy of the activity per each stream subscribed.
 */
public class SubscriptionsBolt extends BaseRichBolt {
    private OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stream", "activity"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String audience = input.getStringByField("stream");
        JSONObject activity = (JSONObject)input.getValueByField("activity");

        // TODO: Don't emit the same audience as stream, instead of that, search all subscriptions.
        this._collector.emit(input, new Values(audience, activity));
        this._collector.ack(input);
    }
}
