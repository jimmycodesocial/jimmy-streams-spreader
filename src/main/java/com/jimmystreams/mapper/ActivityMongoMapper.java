/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.mapper;

import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.tuple.ITuple;
import org.apache.log4j.Logger;

import org.bson.Document;
import org.json.JSONObject;
import com.mongodb.util.JSON;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;
import java.text.SimpleDateFormat;

/**
 * MongoDB Mapper to translate a tuple to a Document.
 * This mapper requires the presence of fields "stream" and "activity" in the tuple.
 */
public class ActivityMongoMapper implements MongoMapper {
    private final static Logger logger = Logger.getLogger(ActivityMongoMapper.class);

    @Override
    public Document toDocument(ITuple tuple) {
        String stream = tuple.getStringByField("stream");
        JSONObject activity = (JSONObject)tuple.getValueByField("activity");
        String activity_id = activity.getString("aid");

        DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'", Locale.ENGLISH);
        Date published = null;

        try {
            published = date_format.parse(activity.getString("published"));
        } catch (ParseException e) {
            logger.error(String.format("Error mapping activity %s to redis: %s", activity_id, e.toString()));
        }

        // Extract fields "aid" and "published" from the activity and save them at document-level.
        // This is for easing the queries.
        // The field "published" was converted to Date, so MongoDB can store it correctly.
        Document doc = new Document("aid", activity.getString("aid"))
                .append("published", published)
                .append("stream", stream)
                .append("activity", JSON.parse(activity.toString()));

        logger.info(String.format("Storing activity %s in historical list of stream %s", activity_id, stream));

        return new Document("$set", doc);
    }
}
