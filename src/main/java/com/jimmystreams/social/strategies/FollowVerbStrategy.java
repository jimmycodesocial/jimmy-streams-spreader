/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class FollowVerbStrategy extends BaseActivityVerbStrategy
{

    public FollowVerbStrategy(OrientGraph graph) {
        super(graph);
    }

    @Override
    protected void acknowledgeActivity(JSONObject activity, OrientVertex actor, OrientVertex object) {
        if (object.getLabel().equals("Technology"))
        {
            logger.info("Relate together technologies of actor");
            OMultiCollectionIterator vertices = (OMultiCollectionIterator)actor.getVertices(Direction.OUT, "interested_in");
            List<OrientVertex> technologies = new ArrayList<>();

            while (vertices.hasNext()) {
                technologies.add((OrientVertex)vertices.next());
            }
            this.connectTechnologies(technologies);
        }

    }

    @Override
    protected Boolean isVerbEdgeable() {
        return true;
    }

    @Override
    protected Boolean isObjectReputationAffectedByVerb() {
        return true;
    }

    @Override
    protected Boolean existObjectTypeInGraph(JSONObject object) {
        return true;
    }

    @Override
    protected double getReputationValueForActor() {
        return REPUTATION_FACTOR * 2.0;
    }

    @Override
    protected double getReputationValueForObject() {
        return REPUTATION_FACTOR * 10.0;
    }

    @Override
    protected String getGraphLabel(JSONObject activity)
    {
        JSONObject object = (JSONObject)activity.get("object");

        String type = object.get("objectType").toString();
        if (type.equals("user"))
            return "follows";
        if (type.equals("technology"))
            return "interested_in";

        return null;
    }
}
