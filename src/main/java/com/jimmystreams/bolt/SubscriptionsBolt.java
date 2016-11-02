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
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bolt that listen for audiences and retrieve the list of streams subscribed to the audience.
 * This bolt will emit a copy of the activity per each stream subscribed.
 */
public class SubscriptionsBolt extends BaseRichBolt {
    private String dsn;
    private String user;
    private String password;

    private int batch;
    private ODatabaseDocumentTx _connection;
    private OutputCollector _collector;

    private final static Logger logger = Logger.getLogger(SubscriptionsBolt.class);

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
        String audience = input.getStringByField("stream");
        JSONObject activity = (JSONObject)input.getValueByField("activity");

        logger.info(String.format("Find streams subscribed to %s", audience));

        int page = 0;
        List<ODocument> results;

        do {
            results = paginateSubscriptions(audience, page, this.batch);
            page++;
            for (ODocument o : results) {
                this._collector.emit(input, new Values(o.<String>field("id"), activity));
            }
        } while (results.size() == this.batch);

        this._collector.ack(input);
    }

    private List<ODocument> paginateSubscriptions(String stream, int page, int amount) {
        Map<String, Object> params = new HashMap<>();
        params.put("starter", stream);
        params.put("notification", false);
        params.put("offset", page * amount);
        params.put("quantity", amount);

        this._connection.activateOnCurrentThread();

        return this._connection.command(new OCommandFunction("findSubscriptions")).execute(params);
    }
}
