/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.json.JSONObject;

public class VoteVerbStrategy extends BaseActivityVerbStrategy
{
    public VoteVerbStrategy(OrientGraph graph) {
        super(graph);
    }

    @Override
    protected Boolean isVerbEdgeable() {
        return false;
    }


    @Override
    protected Boolean existObjectTypeInGraph(JSONObject object) {
        return object.getString("objectType").equals("post") || object.getString("objectType").equals("article") ;
    }

    @Override
    protected double getReputationValueForActor() {
        return REPUTATION_FACTOR * 1.0;
    }

    @Override
    protected void acknowledgeActivity(JSONObject activity, OrientVertex actor, OrientVertex object)
    {

        if (!object.getLabel().equals("Article")) {
            return;
        }

        double likeValue = activity.getString("verb").equals("upvote") ? 1 : -1;

        double score = object.getProperty("score");
        object.setProperty("score", score + likeValue * REPUTATION_FACTOR * 1.0);
    }

    @Override
    protected JSONObject getActivitySocialObject(JSONObject activity)
    {
        JSONObject object =  activity.getJSONObject("object");
        object.put("objectType", "article");

        return object;
    }

}
