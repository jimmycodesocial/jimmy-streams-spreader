package com.jimmystreams.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.apache.log4j.Logger;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

public class SubscriptionsBolt extends BaseBasicBolt {
    final static Logger logger = Logger.getLogger(SubscriptionsBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stream", "activity"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // {"id": "<id>", "name": "<name>"}
        JSONObject audience = (JSONObject)input.getValueByField("audience");
        JSONObject activity = (JSONObject)input.getValueByField("activity");

        // TODO: Don't emit the same audience as stream, instead of that, search all subscriptions.
        collector.emit(new Values(audience, activity));
    }
}
