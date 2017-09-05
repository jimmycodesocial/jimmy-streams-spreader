/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.bolt;

import com.orientechnologies.orient.core.command.script.OCommandFunction;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.apache.storm.tuple.Values;
import org.bson.Document;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Bolt that listen for audiences and retrieve the list of streams subscribed to the audience.
 * This bolt will emit a copy of the activity per each stream subscribed.
 */
    public class SubscriptionsBolt extends BaseRichBolt {
    private String dsn;
    private String user;
    private String password;

    private ODatabaseDocumentTx _connection;
    protected int batch;
    protected OutputCollector _collector;

    protected final static Logger logger = Logger.getLogger(SubscriptionsBolt.class);

    public SubscriptionsBolt(String dsn, String user, String password) {
        this.dsn = dsn;
        this.user = user;
        this.password = password;
        this.batch = 100;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stream", "activity"));
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        this.batch = ((Long)conf.get("stream_orientdb_batch")).intValue();
        this._connection = new ODatabaseDocumentTx(this.dsn);
        this._connection.open(this.user, this.password);
    }

    @Override
    public void execute(Tuple input) {
        Document stream = (Document)input.getValueByField("stream");
        JSONObject activity = (JSONObject)input.getValueByField("activity");

        logger.info(String.format("Find streams subscribed to %s", stream.getString("id")));

        int page = 0;
        List<ODocument> results;

        DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'", Locale.ENGLISH);
        Date published = new Date();

        try {
            published = date_format.parse(activity.getString("published"));
        } catch (ParseException e) {
            logger.warn(String.format("Error parsing date from activity <%s>", activity.getString("published")));
            logger.warn("Use <new Date()> instead");
        }

        do {
            results = paginateSubscriptions(stream.getString("id"), false, published, page, this.batch);
            page++;
            for (ODocument o : results) {
                if (!activity.getJSONObject("actor").getString("id").equals(o.<String>field("id"))) {
                    this._collector.emit(input, new Values(o.<String>field("id"), activity));
                }
            }
        } while (results.size() == this.batch);

        // If need persistence, save the stream in Redis & Mongo
        if (stream.getBoolean("persist")) {
            this._collector.emit(input, new Values(stream.getString("id"), activity));
        }

        this._collector.ack(input);
    }

    protected List<ODocument> paginateSubscriptions(String stream, boolean notification, Date published, int page, int amount) {
        Map<String, Object> params = new HashMap<>();
        params.put("starter", stream);
        params.put("notification", notification);
        params.put("time_mark", published.getTime());
        params.put("offset", page * amount);
        params.put("quantity", amount);

        this._connection.activateOnCurrentThread();

        return this._connection.command(new OCommandFunction("findSubscriptions")).execute(params);
    }
}
