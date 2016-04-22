package com.jimmystreams.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.log4j.Logger;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONStringer;

import java.util.ArrayList;
import java.util.List;

public class AudienceBolt extends BaseBasicBolt {

    private String[] implicitAudiences = {"actor", "object", "target"};
    private String[] explicitAudiences = {"to", "bto", "cc", "bcc"};

    final static Logger logger = Logger.getLogger(AudienceBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stream", "activity"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // Activity JSON
        JSONObject activity = (JSONObject)input.getValueByField("activity");

        // Search the list of audiences present in the activity
        List<JSONObject> audiences = this.extractAudiencesFromActivity(activity);

        // Convert each audience specification to stream notation
        for (JSONObject audience: audiences) {

            // Check for a correct definition of audience
            if (audience.has("id") && audience.has("objectType")) {

                // Convert audience to stream notation
                String stream = new JSONStringer()
                        .object()
                            .key("name").value(audience.get("id"))
                            .key("type").value(audience.get("objectType"))
                        .endObject()
                        .toString();

                // Emit the tuple <stream, activity>
                logger.info(String.format("Audience: %s for activity: %s", stream, activity.get("aid")));
                collector.emit(new Values(new JSONObject(stream), activity));
            }
        }
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
