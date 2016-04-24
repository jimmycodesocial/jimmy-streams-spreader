/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.spout;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.log4j.Logger;

import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class SqsPoolSpout extends BaseRichSpout {
    private final String queueUrl;
    private int sleepTime = 2000;
    private int batch = 5;
    private final boolean reliable;

    private SpoutOutputCollector collector;
    private AmazonSQSAsync sqs;
    private LinkedBlockingQueue<Message> queue;

    private final static Logger logger = Logger.getLogger(SqsPoolSpout.class);

    /**
     * @param queueUrl URL for Amazon SQS queue to consume from
     * @param reliable Uses Storm's reliability facilities?
     */
    public SqsPoolSpout(String queueUrl, boolean reliable) {
        this.queueUrl = queueUrl;
        this.reliable = reliable;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.queue = new LinkedBlockingQueue<>();
        this.sqs = new AmazonSQSAsyncClient(new ProfileCredentialsProvider());
        this.sleepTime = ((Long)conf.get("sqs_sleep_time")).intValue();
        this.batch = ((Long)conf.get("sqs_batch")).intValue();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("activity"));
    }

    @Override
    public void nextTuple() {
        // Look for more messages when the last messages were processed
        if (this.queue.isEmpty()) {
            // Request the queue for more messages
            ReceiveMessageResult receiveMessageResult = this.sqs.receiveMessage(
                    new ReceiveMessageRequest(this.queueUrl).withMaxNumberOfMessages(this.batch));

            // Store the messages locally in memory
            this.queue.addAll(receiveMessageResult.getMessages());
        }

        // Extract one message from the memory queue
        Message message = this.queue.poll();

        if (message != null) {
            // Unique identifier for the message.
            String msgId = message.getMessageId();
            // Identifier associated with the act of receiving the message.
            // A new receipt handle is returned every time you receive a message.
            String msgHandler = message.getReceiptHandle();

            // Parse the message and convert into a tuple
            Values tuple = messageToTuple(message);

            // Fail when the message cannot be parsed
            if (tuple == null) {
                logger.error(String.format("Wrong format for message with id %s and handler %s", msgId, msgHandler));
                this.fail(msgHandler);
            }
            else {
                // Process in a reliable mode
                if (this.reliable) {
                    logger.info(String.format("Emit activity in reliable mode for processing. %s", tuple));
                    collector.emit(tuple, msgHandler);
                }
                // Give ack anyway
                else {
                    logger.info(String.format("Emit activity for processing. %s", tuple));
                    this.sqs.deleteMessageAsync(new DeleteMessageRequest(this.queueUrl, msgHandler));
                    collector.emit(tuple);
                }
            }
        }
        // Origin queue and memory queue are empty
        else {
            // The origin queue is empty, go to sleep.
            logger.warn(String.format("No messages to process. Sleep for %d seconds", this.sleepTime));
            Utils.sleep(this.sleepTime);
        }
    }

    /**
     * Transform a SQS message into a Storm Tuple.
     *
     * @param message The SQS message.
     *
     * @return Values The tuple.
     */
    private Values messageToTuple(Message message) {
        String msgHandler = message.getReceiptHandle();
        String msgId = message.getMessageId();

        // Read the message (JSON Body)
        String rawBody = message.getBody();

        logger.info(String.format("Processing message with id %s and handler %s", msgId, msgHandler));
        logger.info(rawBody);

        JSONObject jsonBody = new JSONObject(rawBody);
        Values tuple = new Values(jsonBody);

        return tuple;
    }

    /**
     * Returns the number of milliseconds the spout will wait before making
     * another call to SQS when the previous call came back empty.
     *
     * Since Amazon charges per SQS request, you can use this parameter to
     * control costs for lower-volume queues.
     *
     * @return The number of milliseconds the spout will wait between SQS calls.
     */
    public int getSleepTime() {
        return this.sleepTime;
    }

    /**
     * Sets the number of milliseconds the spout will wait before making
     * another call to SQS when the previous call came back empty.
     *
     * Since Amazon charges per SQS request, you can use this parameter to
     * control costs for lower-volume queues.
     *
     * @param sleepTime The number of milliseconds the spout will wait between SQS calls.
     */
    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public void ack(Object msgHandler) {
        // Only called in reliable mode.
        try {
            logger.info(String.format("Ack for %s", msgHandler));
            this.sqs.deleteMessageAsync(new DeleteMessageRequest(this.queueUrl, (String) msgHandler));
        }
        catch (AmazonClientException e) {
            logger.error(String.format("AWS Exception %s", e.toString()));
        }
    }

    @Override
    public void fail(Object msgHandler) {
        // Only called in reliable mode.
        try {
            logger.warn(String.format("Message %s fails", msgHandler));
            this.sqs.changeMessageVisibilityAsync(new ChangeMessageVisibilityRequest(this.queueUrl, (String) msgHandler, 0));
        }
        catch (AmazonClientException e) {
            logger.error(String.format("AWS Exception %s", e.toString()));
        }
    }

    @Override
    public void close() {
        this.sqs.shutdown();
        // Works around a known bug in the Async clients
        // @see https://forums.aws.amazon.com/thread.jspa?messageID=305371
        ((AmazonSQSAsyncClient) sqs).getExecutorService().shutdownNow();
    }
}