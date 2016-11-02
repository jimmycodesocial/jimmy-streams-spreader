/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.json.JSONObject;

public class ReadVerbStrategy extends BaseActivityVerbStrategy
{

    public ReadVerbStrategy(OrientGraph graph) {
        super(graph);
    }

    @Override
    protected Boolean isVerbEdgeable() {
        return true;
    }

    @Override
    protected Boolean existObjectTypeInGraph(JSONObject object)
    {
        return object.get("objectType").equals("post") || object.get("objectType").equals("article");
    }

    @Override
    protected double getReputationValueForActor() {
        return REPUTATION_FACTOR * 0.01;
    }


    @Override
    protected Boolean isObjectReputationAffectedByVerb() {
        return true;
    }

    @Override
    protected double getReputationValueForObject() {
        return REPUTATION_FACTOR * 1.0;
    }

    @Override
    protected String getVertexClass(JSONObject vertex)
    {
        String type = vertex.get("objectType").toString();
        return type.equals("post") || type.equals("article") ? "Article" : super.getVertexClass(vertex);
    }

    @Override
    protected String getGraphLabel(JSONObject activity)
    {
        return "view";
    }

    @Override
    protected void acknowledgeActivity(JSONObject activity, OrientVertex actor, OrientVertex object)
    {
        double score = object.getProperty("score");
        object.setProperty("score", score + REPUTATION_FACTOR * 1.0);
    }
}
