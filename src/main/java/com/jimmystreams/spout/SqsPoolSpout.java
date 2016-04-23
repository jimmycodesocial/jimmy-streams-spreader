/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

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

public class SqsPoolSpout extends BaseRichSpout {
    final static Logger logger = Logger.getLogger(SqsPoolSpout.class);

    private SpoutOutputCollector collector;
    private AmazonSQSAsync sqs;
    private LinkedBlockingQueue<Message> queue;

    private final String queueUrl;
    private final boolean reliable;
    private int sleepTime;
    private int batch;

    /**
     * @param queueUrl URL for Amazon SQS queue to consume from
     * @param reliable Uses Storm's reliability facilities?
     */
    public SqsPoolSpout(String queueUrl, boolean reliable) {
        this.queueUrl = queueUrl;
        this.reliable = reliable;
        this.sleepTime = 2000;
        this.batch = 5;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        queue = new LinkedBlockingQueue<>();
        sqs = new AmazonSQSAsyncClient(new ProfileCredentialsProvider());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("activity"));
    }

    @Override
    public void nextTuple() {
        // Look for more messages when the last messages were processed
        if (queue.isEmpty()) {
            // Request the queue for more messages
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(this.batch));
            // Store the messages locally in memory
            queue.addAll(receiveMessageResult.getMessages());
        }

        // Extract one message from the memory queue
        Message message = queue.poll();

        if (message != null) {
            // Parse the message and convert into a tuple
            Values tuple = messageToTuple(message);

            // Fail when the message cannot be parsed
            if (tuple == null) {
                logger.error(String.format("Wrong format for message %s", message.getMessageId()));
                this.fail(message.getMessageId());
            }
            else {
                // Process in a reliable mode
                if (reliable) {
                    logger.info(String.format("Emit activity in reliable mode for processing. %s", tuple));
                    collector.emit(tuple, message.getReceiptHandle());
                }
                // Give ack anyway
                else {
                    logger.info(String.format("Emit activity for processing. %s", tuple));
                    sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
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
     * Transform a SQS message into a Storm Tuple
     *
     * @param message the SQS message
     *
     * @return Values the tuple
     */
    private Values messageToTuple(Message message) {
        // This is and identifier that SQS generate to handle the message status.
        String receiptHandle = message.getReceiptHandle();
        // Read the message (JSON Body)
        String rawBody = message.getBody();

        logger.info(String.format("Processing message with handler %s", receiptHandle));
        logger.info(rawBody);

        JSONObject jsonBody = new JSONObject(rawBody);
        Values tuple = new Values(jsonBody);

        return tuple;
    }

    /**
     * Returns the number of milliseconds the spout will wait before making
     * another call to SQS when the previous call came back empty. Defaults to
     * {@code 100}.
     *
     * Since Amazon charges per SQS request, you can use this parameter to
     * control costs for lower-volume queues.
     *
     * @return the number of milliseconds the spout will wait between SQS calls.
     */
    public int getSleepTime() {
        return sleepTime;
    }

    /**
     * Sets the number of milliseconds the spout will wait before making
     * another call to SQS when the previous call came back empty.
     *
     * Since Amazon charges per SQS request, you can use this parameter to
     * control costs for lower-volume queues.
     *
     * @param sleepTime the number of milliseconds the spout will wait between SQS calls.
     */
    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public void ack(Object msgId) {
        // Only called in reliable mode.
        try {
            logger.info(String.format("Ack for %s", msgId));
            sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, (String) msgId));
        }
        catch (AmazonClientException e) {
            logger.error(String.format("AWS Exception %s", e.toString()));
        }
    }

    @Override
    public void fail(Object msgId) {
        // Only called in reliable mode.
        try {
            logger.warn(String.format("Message %s fails", msgId));
            sqs.changeMessageVisibilityAsync(new ChangeMessageVisibilityRequest(queueUrl, (String) msgId, 0));
        }
        catch (AmazonClientException e) {
            logger.error(String.format("AWS Exception %s", e.toString()));
        }
    }

    @Override
    public void close() {
        sqs.shutdown();
        // Works around a known bug in the Async clients
        // @see https://forums.aws.amazon.com/thread.jspa?messageID=305371
        ((AmazonSQSAsyncClient) sqs).getExecutorService().shutdownNow();
    }
}