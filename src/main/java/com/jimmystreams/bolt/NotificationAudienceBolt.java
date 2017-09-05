package com.jimmystreams.bolt;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

        String audience = this.getNotificationAudience(activity);

        this.findSubscriptionsAndEmitTuple(tuple, activity, audience);

        // Ack the tuple.
        this._collector.ack(tuple);
    }

    private void findSubscriptionsAndEmitTuple(Tuple tuple, JSONObject activity, String stream) {
        List<ODocument> results;
        int page = 0;

        DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'", Locale.ENGLISH);
        Date published = new Date();

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
                    this._collector.emit(tuple, new Values(o.<String>field("id"), activity));
                }
            }
        } while (results.size() == this.batch);
    }

    private String getNotificationAudience(JSONObject activity) {

        String verb = activity.getString("verb");
        String audienceField;

        switch (verb) {
            case "comment":
                audienceField = "target";
                break;
            default:
                audienceField = "object";
        }

        return ((JSONObject)activity.get(audienceField)).getString("id");
    }
}
