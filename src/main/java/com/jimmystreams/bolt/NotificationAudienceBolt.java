package com.jimmystreams.bolt;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class NotificationAudienceBolt extends SubscriptionsBolt {

    public NotificationAudienceBolt(String dsn, String user, String password) {
        super(dsn, user, password);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("user", "activity"));
    }

    @Override
    public void execute(Tuple tuple) {
        JSONObject activity = (JSONObject)tuple.getValueByField("activity");

        List<JSONObject> audience = this.getNotificationAudience(activity);

        for (JSONObject user: audience) {
            this.findSubscriptionsAndEmitTuple(tuple, activity, user);
        }

        // Ack the tuple.
        this._collector.ack(tuple);
    }

    private void findSubscriptionsAndEmitTuple(Tuple tuple, JSONObject activity, JSONObject user) {
        List<ODocument> results;
        int page = 0;

        DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'", Locale.ENGLISH);
        Date published = new Date();
        String stream = user.getString("id");

        try {
            published = date_format.parse(activity.getString("published"));
        } catch (ParseException e) {
            logger.warn(String.format("Error parsing date from activity <%s>", activity.getString("published")));
            logger.warn("Use <new Date()> instead");
        }
        do {
            results = paginateSubscriptions(stream, true, published, page, this.batch);
            page++;
            for (ODocument o : results) {
                if (!activity.getJSONObject("actor").getString("id").equals(o.<String>field("id"))) {

                    this._collector.emit(tuple, new Values(
                            (new JSONObject())
                                    .put("id", o.<String>field("id"))
                                    .put("notificationType", user.getString("notificationType")),
                            activity
                    ));
                }
            }
        } while (results.size() == this.batch);
    }

    private List<JSONObject> getNotificationAudience(JSONObject activity) {

        String verb = activity.getString("verb");
        String audienceField;

        switch (verb) {
            case "comment":
                audienceField = "target";
                break;
            default:
                audienceField = "object";
        }

        List<JSONObject> audience = new ArrayList<>();

        audience.add(
            (new JSONObject())
                .put("id", activity.getJSONObject(audienceField).getString("id"))
                .put("notificationType", activity.getString("verb"))
        );

        // If verb is publish, mentions should also be notified.
        if (verb.equals("publish") && activity.has("to")) {
            JSONArray to = activity.getJSONArray("to");

            for (Object user: to) {
                if (((JSONObject) user).getString("objectType").equals("user")) {
                    audience.add(
                        (new JSONObject())
                                .put("id", ((JSONObject) user).getString("id"))
                                .put("notificationType", "mention")
                    );
                }
            }
        }

        return audience;
    }
}
