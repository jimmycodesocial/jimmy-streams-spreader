package com.jimmystreams.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class NotificationRedisDealerBolt extends BaseRichBolt{

    private OutputCollector collector;
    private JedisCluster client;
    private Set<HostAndPort> jedisClusterNodes;

    public NotificationRedisDealerBolt(Set<HostAndPort> nodes) {
        this.jedisClusterNodes = nodes;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.client = new JedisCluster(this.jedisClusterNodes);
        this.collector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

    @Override
    public void execute(Tuple tuple) {
        Map<String, JedisPool> nodeMap = this.client.getClusterNodes();

        List<JedisPool> nodePoolList = new ArrayList<>(nodeMap.values());
        Collections.shuffle(nodePoolList);

        JedisPool one = nodePoolList.get(0);

        String user = tuple.getStringByField("user");
        String messageType = tuple.getStringByField("messageType");
        Document message = new Document("type", messageType);
        one.getResource().publish(user, message.toString());

        this.collector.ack(tuple);
    }
}
