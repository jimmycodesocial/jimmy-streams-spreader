package com.jimmystreams.bolt;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.json.JSONObject;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class NotificationMongoDealerBolt extends BaseRichBolt{

    private static final int TIME_WINDOW_SIZE = 12; //12 hrs windows size

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
        outputFieldsDeclarer.declare(new Fields("notification"));

    }

    @Override
    public void execute(Tuple tuple) {
        String user = tuple.getStringByField("user");
        JSONObject activity = (JSONObject)tuple.getValueByField("activity");

        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.upsert(true);

        Document filters = this.buildNotificationFilters(user, activity);
        Document updatedNotification = this.buildNotificationUpdatedDocument(user, activity);

        this.collection.findOneAndUpdate(filters, updatedNotification, options);

        // emit tuple

        this.collector.ack(tuple);
    }

    private Document buildNotificationFilters(String user, JSONObject activity) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR, -1 * TIME_WINDOW_SIZE);
        Date fromDate = cal.getTime();

        return (new Document())
                .append("user", user)
                .append("type", activity.getString("verb"))
                .append("object.id", ((JSONObject)activity.get("object")).getString("id"))
                .append("updatedAt", new Document("$gte", fromDate))
        ;
    }

    private Document buildNotificationUpdatedDocument(String user, JSONObject activity) {
        Calendar cal = Calendar.getInstance();

        Document updated = (new Document())
                .append("user", user)
                .append("type", activity.getString("type"))
                .append("object", activity.get("object"))
                .append("updatedAt", cal.getTime())
                .append("$inc", new Document("times", 1)) //increment times aggregated
                .append("$push", new Document("who", new Document("$position", 0)))
        ;

        if (activity.has("target")) {
            updated.append("target", activity.get("target"));
        }

        return updated;
    }

}
