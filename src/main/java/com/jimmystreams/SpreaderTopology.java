/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams;

import com.jimmystreams.bolt.*;
import com.mongodb.client.model.Filters;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.mongodb.bolt.MongoUpdateBolt;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.LocalCluster;

import com.jimmystreams.spout.SqsPoolSpout;
import com.jimmystreams.mapper.ActivityMongoMapper;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.*;

class SpreaderTopology implements Serializable {
    private static Properties prop = new Properties();

    /**
     * Topology startup point.
     * Create, configure and submit the topology.
     */
    public static void main(String[] args) throws IOException, TException{
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

        String streamGraph = prop.getProperty("stream_graph");
        // Extract the notification audience from the activity.
        builder.setBolt("notification_audience",
                new NotificationAudienceBolt(
                        getOrientDBDsn(streamGraph),
                        getOrientDBUser(streamGraph),
                        getOrientDBPassword(streamGraph)
                ), 1)
                .shuffleGrouping("activities");

        // Save the notification in MongoDB
        builder.setBolt("notification_historic",
                new NotificationMongoDealerBolt(
                        getMongoDBDsn(),
                        getMongoDBNotificationsCollection()
                ), 1)
                .shuffleGrouping("notification_audience");

        // Save users interactions into the Social Graph
        String socialGraph = prop.getProperty("social_graph");
        builder.setBolt("social",
                new SocialActivityBolt(
                        getOrientDBDsn(socialGraph),
                        getOrientDBUser(socialGraph),
                        getOrientDBPassword(socialGraph)
                ), 1)
                .shuffleGrouping("activities");

        // Look for all streams subscribed to the audience.
        // Read subscriptions from OrientDB database.
        builder.setBolt("subscriptions",
                new SubscriptionsBolt(
                        getOrientDBDsn(streamGraph),
                        getOrientDBUser(streamGraph),
                        getOrientDBPassword(streamGraph)
                ), 3)
                .setNumTasks(6)
                .shuffleGrouping("audience", "timeline");
        // Store activity logs for explicit audience in other collection

        // Filters to update or create an activity on Mongo Collection
        QueryFilterCreator updateQueryFilters = (QueryFilterCreator) tuple -> {

            // Get data needed form Tuple
            JSONObject activity = (JSONObject)tuple.getValueByField("activity");
            String stream = tuple.getValueByField("stream").toString();

            return Filters.and(
                    Filters.eq("aid", activity.getString("aid")),
                    Filters.eq("stream", stream)
            );
        };

        MongoUpdateBolt activityLogBolt = new MongoUpdateBolt(
                getMongoDBDsn(),
                getMongoDBActivitiesCollection(),
                updateQueryFilters,
                new ActivityMongoMapper()
        );
        activityLogBolt.withUpsert(true);

        builder.setBolt("activityLog", activityLogBolt, 4)
                .setNumTasks(8)
                .shuffleGrouping("audience", "activityLog");

        // Store the activity as historical for the streams.

        MongoUpdateBolt timelineBolt = new MongoUpdateBolt(
                getMongoDBDsn(),
                getMongoDBTimeLineCollection(),
                updateQueryFilters,
                new ActivityMongoMapper()
        );
        timelineBolt.withUpsert(true);
        builder.setBolt("timeline", timelineBolt, 4)
                .setNumTasks(8)
                .shuffleGrouping("subscriptions");

        // Store the most recent activities for the streams.
        // This bolt should be processed as soon as possible.
        // Limit streams up-to 1000 entries.
        builder.setBolt("recent",
                new RedisUpdatesBolt(getRedisClusterConfig(), 1000), 8)
                .setNumTasks(16)
                .shuffleGrouping("subscriptions");

        builder.setBolt("publish_notification",
                new NotificationRedisDealerBolt(getRedisClusterInitialNodes()), 1)
                .shuffleGrouping("notification_historic")
                .shuffleGrouping("recent");


        // Submit the topology
        Config conf = getTopologyConfig();
        StormTopology topology = builder.createTopology();

        if (Integer.parseInt(prop.getProperty("production")) == 1) {

            List<String> seeds = new ArrayList<>();
            seeds.add(prop.getProperty("nimbus_ip"));
            conf.put(Config.NIMBUS_SEEDS, seeds);

            conf.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(prop.getProperty("nimbus_port")));

            conf.setNumWorkers(1);
            conf.setMaxSpoutPending(5000);
            StormSubmitter.submitTopology(prop.getProperty("topology"), conf, topology);
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(prop.getProperty("topology"), conf, topology);

            try {
                Thread.sleep(5000000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                cluster.shutdown();
            }
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
    private static String getMongoDBActivitiesCollection() {
        return prop.getProperty("mongodb_activities_collection");
    }

    /**
     * MongoDB collection where activities will be stored.
     *
     * @return The MongoDB collection name
     */
    private static String getMongoDBTimeLineCollection() {
        return prop.getProperty("mongodb_timeline_collection");
    }

    /**
     * MongoDB collection where activities will be stored.
     *
     * @return The MongoDB collection name
     */
    private static String getMongoDBNotificationsCollection() {
        return prop.getProperty("mongodb_notifications_collection");
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
     * Configuration for redis cluster using Jedis client.
     *
     * @return The redis configuration.
     */
    private static JedisClusterConfig getRedisClusterConfig() {
        JedisClusterConfig.Builder configBuilder = new JedisClusterConfig.Builder();

        Set<InetSocketAddress> nodes = new HashSet<>();
        nodes.add(new InetSocketAddress(prop.getProperty("redis_host"), Integer.valueOf(prop.getProperty("redis_port"))));

        configBuilder.setNodes(nodes);

        return configBuilder.build();
    }

    private static Map<String, Integer> getRedisClusterInitialNodes() {
        Map<String, Integer> nodes = new HashMap<>();
        nodes.put(prop.getProperty("redis_host"), Integer.valueOf(prop.getProperty("redis_port")));
        return nodes;
    }

    /**
     * OrientDB connection string.
     *
     * @return The connection string.
     */
    private static String getOrientDBDsn(String graph) {
        return prop.getProperty(String.format("%s_orientdb_dsn", graph));
    }

    /**
     * OrientDB authentication user.
     *
     * @return The user.
     */
    private static String getOrientDBUser(String graph) {
        return prop.getProperty(String.format("%s_orientdb_user", graph));
    }

    /**
     * OrientDB authentication password.
     *
     * @return The password.
     */
    private static String getOrientDBPassword(String graph) {
        return prop.getProperty(String.format("%s_orientdb_password", graph));
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
        conf.put("access_key", prop.getProperty("access_key"));
        conf.put("secret_key", prop.getProperty("secret_key"));

        // Size of requests to OrientDB.
        conf.put("stream_orientdb_batch", Integer.valueOf(prop.getProperty("stream_orientdb_batch")));

        return conf;
    }
}