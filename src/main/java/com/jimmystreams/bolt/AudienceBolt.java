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

import org.bson.Document;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.*;

/**
 * Bolt that listen for activities and extract the implicit and explicit audiences.
 * This bolt will emit a copy of the activity per each audience identified.
 *
 * Implicit audience fields: "actor", "object", "target"
 * Explicit audience fields: "to", "bto", "cc", "bcc"
 *
 * see: http://activitystrea.ms/specs/json/1.0/#activity
 * see: http://activitystrea.ms/specs/json/targeting/1.0/#properties
 */
public class AudienceBolt extends BaseRichBolt {
    private String[] implicitAudiences = new String[]{"actor"};
    private String[] explicitAudiences = new String[]{"to", "bto", "cc", "bcc"};
    private HashSet<Object> ignoreVerbs = new HashSet<>(Arrays.asList(new String[] {"read", "unfollow"}));
    private OutputCollector _collector;

    private final static Logger logger = Logger.getLogger(AudienceBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("timeline", new Fields   ("stream", "activity"));
        declarer.declareStream("activityLog", new Fields("stream", "activity"));
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

        // Ignore activities
        if (!this.ignoreActivity(activity)) {

            for (String audience : this.implicitAudiences) {
                if (activity.has(audience)) {
                    String streamId = activity.getJSONObject(audience).getString("id");
                    logger.info(String.format("Audience %s for activity %s", streamId, activity_id));

                    Document stream = new Document("id", streamId).append("persist", false);
                    this._collector.emit("timeline", input, new Values(stream, activity));
                }
            }

            // Include audience by activity verb value
            for (Document audience : this.getActivityAudienceByVerb(activity)) {
                this._collector.emit("timeline", input, new Values(audience, activity));
            }

            // Explicit audiences are lists.
            // Save in mongoDB explicit audience
            for (String audience : this.explicitAudiences) {
                if (activity.has(audience)) {
                    JSONArray list = activity.getJSONArray(audience);
                    for (Object aud : list) {
                        String streamId = ((JSONObject) aud).getString("id");
                        String streamType = ((JSONObject) aud).getString("objectType");
                        logger.info(String.format("Audience %s for activity %s", streamId, activity_id));

                        Document stream = new Document("id", streamId).append("persist", true);
                        this._collector.emit("timeline", input, new Values(stream, activity));

                        if (streamType.toLowerCase().equals("user")) {
                            this._collector.emit("activityLog", input, new Values(streamId, activity));
                        }
                    }
                }
            }
        }

        // Ack the tuple.
        this._collector.ack(input);
    }

    private Document[] getActivityAudienceByVerb(JSONObject activity)
    {
        switch(activity.getString("verb"))
        {
            case "review":
                return new Document[]{
                        new Document("id", activity.getJSONObject("target").getString("id"))
                                .append("persist", false)
                };
            default:
                return new Document[]{};
        }
    }

    private boolean ignoreActivity(JSONObject activity) {
        return this.ignoreVerbs.contains(activity.getString("verb"));
    }
}
