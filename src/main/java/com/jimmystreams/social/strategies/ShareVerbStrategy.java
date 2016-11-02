/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.json.JSONObject;

public class ShareVerbStrategy extends BaseActivityVerbStrategy
{

    public ShareVerbStrategy(OrientGraph graph) {
        super(graph);
    }

    @Override
    protected Boolean isVerbEdgeable() {
        return true;
    }

    @Override
    protected Boolean existObjectTypeInGraph(JSONObject object) {
        String type = object.getString("objectType");
        return type.equals("post") || type.equals("article");
    }

    @Override
    protected double getReputationValueForActor() {
        return REPUTATION_FACTOR * 7.0;
    }

    @Override
    protected String getVertexClass(JSONObject vertex)
    {
        String type = vertex.getString("objectType");
        return type.equals("post") || type.equals("article") ? "Article" : super.getVertexClass(vertex);
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
            target.setProperty("score", score + REPUTATION_FACTOR * 10.0);
        }
    }
}
