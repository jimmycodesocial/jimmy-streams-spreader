/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.json.JSONObject;

public class NoStrategy extends BaseActivityVerbStrategy
{

    public NoStrategy(OrientGraph graph) {
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
        return 0;
    }

    @Override
    public void handleActivity(JSONObject activity) {
        // Nothing to do.
    }
}
