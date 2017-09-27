package com.jimmystreams.bolt;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import java.util.*;

public class NotificationMongoDealerBolt extends BaseRichBolt{

    private static final int TIME_WINDOW_SIZE = 12; //12 hrs windows size

    private static final String NOTIFICATION_MESSAGE_TYPE = "notification";

    private OutputCollector collector;
    private MongoClient client;
    private MongoCollection<Document> collection;

    private String dsn;
    private String collectionName;

    public NotificationMongoDealerBolt(String dsn, String collectionName) {
        this.dsn = dsn;
        this.collectionName = collectionName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        MongoClientURI uri = new MongoClientURI(this.dsn);
        this.client = new MongoClient(uri);
        MongoDatabase db = this.client.getDatabase(uri.getDatabase());
        this.collection = db.getCollection(this.collectionName);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("user", "messageType"));

    }

    @Override
    public void execute(Tuple tuple) {
        JSONObject user = (JSONObject)tuple.getValueByField("user");
        JSONObject activity = (JSONObject)tuple.getValueByField("activity");

        UpdateOptions options = new UpdateOptions();
        options.upsert(true);

        Document filters = this.buildNotificationFilters(user, activity);
        Document updatedNotification = this.buildNotificationUpdatedDocument(user, activity);

        this.collection.updateOne(filters, updatedNotification, options);

        this.collector.emit(tuple, new Values(user.getString("id"), NotificationMongoDealerBolt.NOTIFICATION_MESSAGE_TYPE));

        this.collector.ack(tuple);
    }

    private Document buildNotificationFilters(JSONObject user, JSONObject activity) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR, -1 * TIME_WINDOW_SIZE);
        Date fromDate = cal.getTime();

        return (new Document())
                .append("user", new ObjectId(user.getString("id")))
                .append("type", user.getString("notificationType"))
                .append("object.id", ((JSONObject)activity.get("object")).getString("id"))
                .append("updatedAt", new Document("$gte", fromDate))
        ;
    }

    private Document buildNotificationUpdatedDocument(JSONObject user, JSONObject activity) {
        Calendar cal = Calendar.getInstance();

        JSONObject activityActor = activity.getJSONObject("actor");
        JSONObject activityObject = activity.getJSONObject("object");

        List<String> userList = Arrays.asList(activityActor.getString("id"));
        Document actor = (new Document("$each", userList)).append("$position", 0);
        Document updateInfo = (new Document())
                .append("user", new ObjectId(user.getString("id")))
                .append("type", user.getString("notificationType"))
                .append("object", new Document("id", activityObject.getString("id"))
                        .append("objectType", activityObject.getString("objectType")))
                .append("updatedAt", cal.getTime());

        if (activity.has("target")) {
            JSONObject activityTarget = activity.getJSONObject("target");
            updateInfo.append(
                    "target", (new Document("id", activityTarget.getString("id")))
                            .append("objectType", activityTarget.get("objectType"))
            );
        }

        Document updated = (new Document())
                .append("$set", updateInfo)
                .append("$inc", new Document("times", 1)) //increment times aggregated
                .append("$push", new Document("who", actor))
        ;

        return updated;
    }

}
