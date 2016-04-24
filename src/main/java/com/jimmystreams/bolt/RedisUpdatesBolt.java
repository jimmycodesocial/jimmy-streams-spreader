/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.bolt;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import org.json.JSONObject;
import redis.clients.jedis.JedisCommands;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Locale;

/**
 * Custom Redis bolt for storing activities in redis.
 * This mapper use the redis command ZADD to arrange the activities by date.
 */
public class RedisUpdatesBolt extends AbstractRedisBolt {
    /**
     * Size of the storage. Set to zero for no limits.
     */
    private int maxEntries = 0;

    private final static Logger logger = Logger.getLogger(RedisUpdatesBolt.class);

    /**
     * Default constructor.
     *
     * @param config     The redis connection config.
     * @param maxEntries The size of the storage.
     */
    public RedisUpdatesBolt(JedisPoolConfig config, int maxEntries) {
        super(config);
        this.maxEntries = maxEntries;
    }

    /**
     * Default constructor.
     *
     * @param config The redis connection config.
     */
    public RedisUpdatesBolt(JedisPoolConfig config) {
        this(config, 0);
    }

    @Override
    public void execute(Tuple input) {
        String stream = input.getStringByField("stream");
        JSONObject activity = (JSONObject)input.getValueByField("activity");
        String activity_id = activity.getString("aid");

        // Parse the published date.
        DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'", Locale.ENGLISH);
        Date published = null;

        try {
            published = date_format.parse(activity.getString("published"));
        } catch (ParseException e) {
            logger.error(String.format("Error mapping activity %s to redis: %s", activity_id, e.toString()));
        }

        JedisCommands jedisCommand = getInstance();

        // Only perform the redis command if the published was parsed correctly.
        if (published != null) {
            // ZADD command will add a value to the key using an score for sorting.
            // The score used is the timestamp of when the activity was published.
            logger.info(String.format("Storing activity %s in recent list of stream %s", activity_id, stream));
            jedisCommand.zadd(stream, published.getTime(), activity.toString());

            // Limit the storage.
            if (this.maxEntries != 0) {
                int size = -maxEntries;
                jedisCommand.zremrangeByRank(stream, size, size-2);
            }
        }

        // Acknowledge the tuple.
        collector.ack(input);
        returnInstance(jedisCommand);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
