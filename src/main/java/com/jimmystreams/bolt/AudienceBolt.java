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
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.log4j.Logger;

import org.json.JSONObject;
import org.json.JSONArray;

import java.util.Map;

/**
 * Bolt that listen for activities and extract the implicit and explicit audiences.
 * This bolt will emit a copy of the activity per each audience identified.
 *
 * Implicit audience fields: "actor", "object", "target"
 * Explicit audience fields: "to", "bto", "cc", "bcc"
 */
public class AudienceBolt extends BaseRichBolt {
    private String[] implicitAudiences = {"actor", "object", "target"};
    private String[] explicitAudiences = {"to", "bto", "cc", "bcc"};
    private OutputCollector _collector;

    private final static Logger logger = Logger.getLogger(AudienceBolt.class);

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
        // Activity JSON.
        JSONObject activity = (JSONObject)input.getValueByField("activity");
        String activity_id = activity.getString("aid");

        for (String audience: this.implicitAudiences) {
            if (activity.has(audience)) {
                String stream = activity.getJSONObject(audience).getString("id");
                logger.info(String.format("Audience %s for activity %s", stream, activity_id));
                this._collector.emit(input, new Values(stream, activity));
            }
        }

        // Explicit audiences are lists.
        for (String audience: this.explicitAudiences) {
            if (activity.has(audience)) {
                JSONArray list = activity.getJSONArray(audience);
                for (Object aud: list) {
                    String stream = ((JSONObject)aud).getString("id");
                    logger.info(String.format("Audience %s for activity %s", stream, activity_id));
                    this._collector.emit(input, new Values(stream, activity));
                }
            }
        }

        // Ack the tuple.
        this._collector.ack(input);
    }
}
