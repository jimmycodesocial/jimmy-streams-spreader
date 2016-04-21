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
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import org.apache.log4j.Logger;

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
        this.sleepTime = 100;
        this.batch = 5;
    }

    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        queue = new LinkedBlockingQueue<>();
        sqs = new AmazonSQSAsyncClient(new ProfileCredentialsProvider());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("activity"));
    }

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
            Values tuple = messageToStormTuple(message);

            // Fail when the message cannot be parsed
            if (tuple == null) {
                this.fail(message.getMessageId());
            }
            else {
                // Process in a reliable mode
                if (reliable) {
                    collector.emit(getStreamId(message), tuple, message.getReceiptHandle());
                }
                // Give ack anyway
                else {
                    sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
                    collector.emit(getStreamId(message), tuple);
                }
            }
        }
        // Origin queue and memory queue are empty
        else {
            // The origin queue is empty, go to sleep.
            Utils.sleep(sleepTime);
        }
    }

    /**
     * Transform a SQS message into a Storm Tuple
     *
     * @param message the SQS message
     *
     * @return Values the tuple
     */
    private Values messageToStormTuple(Message message) {
        // This is and identifier that SQS generate to handle the message status.
        String receiptHandle = message.getReceiptHandle();
        // Read the message (JSON Body)
        String rawBody = message.getBody();

        JSONObject jsonBody;
        Values tuple = null;

        logger.info(String.format("Processing message with handler %s", receiptHandle));
        logger.info(rawBody);

        try {
            // Convert the message into a JSON Object
            jsonBody = new JSONObject(rawBody);
            tuple = new Values(jsonBody);
        } catch (JSONException e) {
            logger.error(String.format("Error %s for message %s", e.getMessage(), message.getMessageId()));
        }

        return tuple;
    }

    /**
     * Returns the stream on which this spout will emit. By default, it is just
     * {@code Utils.DEFAULT_STREAM_ID}. Simply override this method to send to
     * a different stream.
     *
     * By using the {@code message} parameter, you can send different messages
     * to different streams based on context.
     *
     * @return the stream on which this spout will emit.
     */
    public String getStreamId(Message message) {
        return Utils.DEFAULT_STREAM_ID;
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

    public void ack(Object msgId) {
        // Only called in reliable mode.
        try {
            sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, (String) msgId));
        }
        catch (AmazonClientException ignored) { }
    }

    public void fail(Object msgId) {
        // Only called in reliable mode.
        try {
            sqs.changeMessageVisibilityAsync(new ChangeMessageVisibilityRequest(queueUrl, (String) msgId, 0));
        }
        catch (AmazonClientException ignored) { }
    }

    public void close() {
        sqs.shutdown();
        // Works around a known bug in the Async clients
        // @see https://forums.aws.amazon.com/thread.jspa?messageID=305371
        ((AmazonSQSAsyncClient) sqs).getExecutorService().shutdownNow();
    }
}