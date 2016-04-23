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
import org.json.JSONStringer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Bolt that listen for activities and extract the implicit and explicit audiences.
 * This bolt will emit a copy of the activity per each audience identified.
 */
public class AudienceBolt extends BaseRichBolt {
    /**
     * These fields represent the implicit audience.
     */
    private String[] implicitAudiences = {"actor", "object", "target"};

    /**
     * Fields with explicit audience.
     */
    private String[] explicitAudiences = {"to", "bto", "cc", "bcc"};

    /**
     * Collector to ACK tuples.
     */
    private OutputCollector _collector;

    /**
     * Logger instance.
     */
    private final static Logger logger = Logger.getLogger(AudienceBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("audience", "activity"));
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

        // Search the list of audiences present in the activity.
        List<JSONObject> audiences = this.extractAudiencesFromActivity(activity);
        logger.info(String.format("%d audiences detected in activity %s", audiences.size(), activity_id));

        // Convert each audience specification to stream notation.
        for (JSONObject audience: audiences) {

            // Check for a correct definition of audience.
            if (audience.has("id") && audience.has("objectType")) {

                // Convert audience to stream notation.
                String stream = new JSONStringer()
                        .object()
                            .key("name").value(audience.get("id"))
                            .key("type").value(audience.get("objectType"))
                        .endObject()
                        .toString();

                // Emit the tuple <stream, activity>
                logger.info(String.format("Audience: %s for activity: %s", stream, activity_id));
                this._collector.emit(input, new Values(new JSONObject(stream), activity));
            }
        }

        // Ack the tuple.
        this._collector.ack(input);
    }

    /**
     * Extract the list of audiences from an activity.
     *
     * @param activity The activity
     *
     * @return The list of audiences
     */
    private List<JSONObject> extractAudiencesFromActivity(JSONObject activity) {
        List<JSONObject> audiences = new ArrayList<>();

        for (String audience: this.implicitAudiences) {
            if (activity.has(audience)) {
                audiences.add(activity.getJSONObject(audience));
            }
        }

        for (String audience: this.explicitAudiences) {
            if (activity.has(audience)) {
                JSONArray list = activity.getJSONArray(audience);
                for (Object aud: list) {
                    audiences.add((JSONObject)aud);
                }
            }
        }

        return audiences;
    }
}
