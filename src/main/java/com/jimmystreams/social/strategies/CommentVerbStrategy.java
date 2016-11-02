/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.json.JSONObject;

public class CommentVerbStrategy extends BaseActivityVerbStrategy
{

    public CommentVerbStrategy(OrientGraph graph) {
        super(graph);
    }

    @Override
    protected Boolean isVerbEdgeable() {
        return false;
    }


    @Override
    protected Boolean existObjectTypeInGraph(JSONObject object) {
        return false;
    }

    @Override
    protected double getReputationValueForActor() {
        return REPUTATION_FACTOR * 1.0;
    }

    @Override
    protected void acknowledgeActivity(JSONObject activity, OrientVertex actor, OrientVertex object)
    {
        JSONObject activityTarget = (JSONObject) activity.get("target");
        if (activityTarget == null){
            return;
        }

        String type = activityTarget.getString("objectType");
        if (!type.equals("post") && !type.equals("article")) {
            return;
        }

        OrientVertex target = this.findOrCreateVertex(activityTarget, "Article");

        if (target != null) {
            double score = target.getProperty("score");
            target.setProperty("score", score + REPUTATION_FACTOR * 3.0);
        }
    }

}
