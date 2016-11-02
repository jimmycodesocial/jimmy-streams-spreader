/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.Direction;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class PublishVerbStrategy extends BaseActivityVerbStrategy
{

    public PublishVerbStrategy(OrientGraph graph){
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
    protected double getReputationValueForActor()
    {
        return REPUTATION_FACTOR * 12;
    }

    @Override
    protected void acknowledgeActivity(JSONObject activity, OrientVertex actor, OrientVertex object)
    {
        List<OrientVertex> technologies = new ArrayList<>();

        if (activity.has("to"))
        {
            JSONArray audience = activity.getJSONArray("to");

            for (Object item : audience) {
                String type = ((JSONObject) item).getString("objectType");
                String className = this.findClassNameByType(type);
                OrientVertex vertex = this.findOrCreateVertex((JSONObject) item, className);

                // Update vertex score
                double score = vertex.getProperty("score");
                vertex.setProperty("score", score + this.findScoreByType(type));

                // The edge never going to repeat on this type of activity
                graph.addEdge(null, object, vertex, this.findEdgeNameByClass(className));

                if (className.equals("Technology"))
                {
                    // Link actor with technology if not exist
                    this.findOrCreateEdge(actor, vertex, "publish_about", Direction.BOTH);

                    // collect technologies
                    technologies.add(vertex);
                }
            }

            this.connectTechnologies(technologies);
        }
    }

    @Override
    protected String getVertexClass(JSONObject vertex)
    {
        String type = vertex.get("objectType").toString();
        return type.equals("post") || type.equals("article") ? "Article" : super.getVertexClass(vertex);
    }

    private String findClassNameByType(String type)
    {
        if (type.equals("technology"))
            return "Technology";

        if (type.equals("user"))
            return "User";

        return null;
    }

    private String findEdgeNameByClass(String className)
    {
        if (className.equals("Technology"))
            return "is_about";

        if (className.equals("User"))
            return "mention";

        return null;
    }

    private double findScoreByType(String type)
    {
        if (type.equals("technology"))
            return REPUTATION_FACTOR * 9;

        if (type.equals("user"))
            return REPUTATION_FACTOR * 6;

        return 0;
    }
}
