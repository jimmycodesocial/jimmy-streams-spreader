
/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.bolt;


import com.jimmystreams.social.ActivityContext;
import com.jimmystreams.social.OrientDBGraph;
import com.jimmystreams.social.strategies.*;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;

public class SocialActivityBolt extends BaseRichBolt
{

    private OrientGraph graph;
    private ActivityContext context;
    private OutputCollector _collector;

    private String dsn;
    private String user;
    private String password;

    public SocialActivityBolt(String dsn, String user, String password) {
        this.dsn = dsn;
        this.user = user;
        this.password = password;
    }

    private final static Logger logger = Logger.getLogger(SocialActivityBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        try {
            this._collector = outputCollector;
            this.graph = OrientDBGraph.create(this.dsn, this.user, this.password);
            this.context = new ActivityContext();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }

    @Override
    public void execute(Tuple tuple)
    {
        JSONObject activity = (JSONObject)tuple.getValueByField("activity");
        String activityVerb = (String)activity.get("verb");

        logger.info(String.format("Select strategy based on the activity verb"));
        String strategy = activityVerb;
        switch (activityVerb) {
            case "publish":
                context.setVerbStrategy(new PublishVerbStrategy(this.graph));
                break;
            case "follow":
                context.setVerbStrategy(new FollowVerbStrategy(this.graph));
                break;
            case "comment":
                context.setVerbStrategy(new CommentVerbStrategy(this.graph));
                break;
            case "read":
                context.setVerbStrategy(new ReadVerbStrategy(this.graph));
                break;
            case "share":
                context.setVerbStrategy(new ShareVerbStrategy(this.graph));
                break;
            case "review":
                context.setVerbStrategy(new ReviewVerbStrategy(this.graph));
                break;
            case "upvote" :
            case "downvote" :
                context.setVerbStrategy(new VoteVerbStrategy(this.graph));
                break;
            default:
                strategy = "default";
                context.setVerbStrategy(new NoStrategy(this.graph));
        }

        logger.info(String.format("%s strategy selected", strategy));
        context.executeStrategy(activity);


        // Ack the tuple.
        this._collector.ack(tuple);
    }
}
