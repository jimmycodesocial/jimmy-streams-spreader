/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams;

import org.apache.storm.Config;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.LocalCluster;

import com.jimmystreams.spout.SqsPoolSpout;
import com.jimmystreams.bolt.AudienceBolt;
import com.jimmystreams.bolt.SubscriptionsBolt;
import com.jimmystreams.bolt.RedisUpdatesBolt;
import com.jimmystreams.mapper.ActivityMongoMapper;

import java.io.IOException;
import java.util.Properties;

class SpreaderTopology {
    private static Properties prop = new Properties();

    /**
     * Topology startup point.
     * Create, configure and submit the topology.
     */
    public static void main(String[] args) throws IOException {
        // Read the configuration file
        prop.load(SpreaderTopology.class.getClassLoader().getResourceAsStream("configuration.properties"));
        TopologyBuilder builder = new TopologyBuilder();

        // Emit activities into the topology.
        builder.setSpout("activities",
                new SqsPoolSpout(getSqsQueue(), true), 1);

        // Extract the audience from the activity.
        builder.setBolt("audience",
                new AudienceBolt(), 1)
                .shuffleGrouping("activities");

        // Look for all streams subscribed to the audience.
        // Read subscriptions from OrientDB database.
        builder.setBolt("subscriptions",
                new SubscriptionsBolt(getOrientDBDsn(), getOrientDBUser(), getOrientDBPassword()), 3)
                .setNumTasks(6)
                .shuffleGrouping("audience");

        // Store the activity as historical for the streams.
        // This bolt can have a little delay storing the activities.
        builder.setBolt("historical",
                new MongoInsertBolt(getMongoDBDsn(), getMongoDBCollection(), new ActivityMongoMapper()), 4)
                .setNumTasks(8)
                .shuffleGrouping("subscriptions");

        // Store the most recent activities for the streams.
        // This bolt should be processed as soon as possible.
        // Limit streams up-to 1000 entries.
        builder.setBolt("recent",
                new RedisUpdatesBolt(getRedisConfig(), 1000), 8)
                .setNumTasks(16)
                .shuffleGrouping("subscriptions");

        // Submit the topology
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("spreader-topology", getTopologyConfig(), builder.createTopology());

        try {
            Thread.sleep(50000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            cluster.shutdown();
        }
    }

    /**
     * MongoDB connection string.
     *
     * @see "http://storm.apache.org/releases/1.0.0/storm-mongodb.html"
     * @see "https://docs.mongodb.org/manual/reference/connection-string/#connections-connection-options"
     *
     * @return The MongoDB connection string
     */
    private static String getMongoDBDsn() {
        return prop.getProperty("mongodb_dsn");
    }

    /**
     * MongoDB collection where activities will be stored.
     *
     * @return The MongoDB collection name
     */
    private static String getMongoDBCollection() {
        return prop.getProperty("mongodb_collection");
    }

    /**
     * AWS SQS queue to read messages.
     *
     * @return The queue from where the spout will emit messages.
     */
    private static String getSqsQueue() {
        return prop.getProperty("sqs_queue");
    }

    /**
     * Configuration for redis using Jedis client.
     *
     * @return The redis configuration.
     */
    private static JedisPoolConfig getRedisConfig() {
        JedisPoolConfig.Builder configBuilder = new JedisPoolConfig.Builder()
                .setHost(prop.getProperty("redis_host"))
                .setPort(Integer.valueOf(prop.getProperty("redis_port")))
                .setDatabase(Integer.valueOf(prop.getProperty("redis_db")));

        if (prop.getProperty("redis_password").length() > 0) {
            configBuilder.setPassword(prop.getProperty("redis_password"));
        }

        return configBuilder.build();
    }

    /**
     * OrientDB connection string.
     *
     * @return The connection string.
     */
    private static String getOrientDBDsn() {
        return prop.getProperty("orientdb_dsn");
    }

    /**
     * OrientDB authentication user.
     *
     * @return The user.
     */
    private static String getOrientDBUser() {
        return prop.getProperty("orientdb_user");
    }

    /**
     * OrientDB authentication password.
     *
     * @return The password.
     */
    private static String getOrientDBPassword() {
        return prop.getProperty("orientdb_password");
    }

    /**
     * Runtime topology configuration.
     *
     * @return The config
     */
    private static Config getTopologyConfig() {
        Config conf = new Config();

        // @see: http://storm.apache.org/releases/1.0.0/Understanding-the-parallelism-of-a-Storm-topology.html
        conf.setNumWorkers(Integer.valueOf(prop.getProperty("topology_workers")));
        conf.setMaxSpoutPending(Integer.valueOf(prop.getProperty("topology_max_spout_pending")));

        // Spout interaction with SQS queue.
        conf.put("sqs_sleep_time", Integer.valueOf(prop.getProperty("sqs_sleep_time")));
        conf.put("sqs_batch", Integer.valueOf(prop.getProperty("sqs_batch")));

        // Size of requests to OrientDB.
        conf.put("orientdb_batch", Integer.valueOf(prop.getProperty("orientdb_batch")));

        return conf;
    }
}