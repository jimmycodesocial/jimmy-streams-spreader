/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social;

import org.json.JSONObject;

public interface ActivityVerbStrategy
{
    void handleActivity(JSONObject activity);
}
