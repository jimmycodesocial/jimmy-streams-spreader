package com.jimmystreams.bolt;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

import com.amazonaws.services.sns.AmazonSNSClient;
import java.util.Map;

public class SNSMessageDealerBolt extends BaseRichBolt {

    private final static Logger logger = Logger.getLogger(SNSMessageDealerBolt.class);
    private AmazonSNSClient snsClient;
    private OutputCollector collector;
    private String topic;

    public SNSMessageDealerBolt(String topic) {
        this.topic = topic;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.snsClient = new AmazonSNSClient(new ProfileCredentialsProvider());
    }

    @Override
    public void execute(Tuple tuple) {
        String user = tuple.getStringByField("user");
        String notificationType = tuple.getStringByField("messageType");

        Document message = (new Document())
                .append("user", user)
                .append("type", notificationType);

        this.snsClient.publish(this.topic, message.toString());

        // Ack the tuple.
        this.collector.ack(tuple);
    }
}
