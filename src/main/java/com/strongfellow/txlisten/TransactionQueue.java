package com.strongfellow.txlisten;

import java.util.HashMap;
import java.util.Map;

import org.bitcoinj.core.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionQueue {

    private static final Logger logger = LoggerFactory.getLogger(TransactionQueue.class);

    private final AmazonSQS sqs = new AmazonSQSClient();
    private final ObjectMapper mapper = new ObjectMapper();

    private String queueUrl = null;

    public TransactionQueue() {
        this.setRegion("us-west-2");
        this.setQueueName("tx");
    }

    public void setRegion(String region) {
        logger.info("sqs using region {}", region);
        this.sqs.setRegion(Region.getRegion(Regions.fromName(region)));
    }

    public void setQueueName(String queueName) {
        logger.info("setting queue name to {}", queueName);
        this.queueUrl = this.sqs.createQueue(queueName).getQueueUrl();
        logger.info("setting queue url to {}", this.queueUrl);

    }

    private final Map<String, String> map = new HashMap<String, String>();

    public synchronized void put(Transaction t) throws JsonProcessingException {
        logger.info("begin putting transaction {}", t.getHashAsString());
        long currentTimeMillis = System.currentTimeMillis();
        this.map.put("time", Long.toString(currentTimeMillis));
        this.map.put("txHash", t.getHashAsString());
        String message = this.mapper.writeValueAsString(map);
        this.sqs.sendMessage(this.queueUrl, message);
        logger.info("finished putting transaction {}", t.getHashAsString());
    }
}
