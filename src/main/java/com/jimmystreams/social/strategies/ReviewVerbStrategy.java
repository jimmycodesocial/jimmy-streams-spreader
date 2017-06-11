/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.json.JSONObject;

public class ReviewVerbStrategy extends BaseActivityVerbStrategy
{

    public ReviewVerbStrategy(OrientGraph graph) {
        super(graph);
    }

    @Override
    protected JSONObject getActivitySocialObject(JSONObject activity)
    {
      return activity.getJSONObject("target");
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
        return object.getString("objectType").equals("technology");
    }

    @Override
    protected double getReputationValueForActor() {
        return REPUTATION_FACTOR * 10.0;
    }

    @Override
    protected double getReputationValueForObject() {
        return REPUTATION_FACTOR * 10.0;
    }
}
