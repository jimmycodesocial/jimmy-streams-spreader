/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social;

import org.json.JSONObject;

public class ActivityContext {
    private ActivityVerbStrategy verbStrategy;

    public ActivityContext(ActivityVerbStrategy strategy){
        this.setVerbStrategy(strategy);
    }

    public ActivityContext() { }

    public void executeStrategy(JSONObject activity){
        verbStrategy.handleActivity(activity);
    }

    public ActivityVerbStrategy getVerbStrategy() {
        return verbStrategy;
    }

    public void setVerbStrategy(ActivityVerbStrategy verbStrategy) {
        this.verbStrategy = verbStrategy;
    }
}
