package com.strongfellow.txlisten;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.bitcoinj.core.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class TransactionStasher {

    private static final Logger logger = LoggerFactory.getLogger(TransactionStasher.class);
    private final AmazonS3 s3 = new AmazonS3Client();

    public void stash(Transaction t) {
        logger.info("begin stashing {}", t.getHashAsString());
        String key = String.format("transactions/%s/payload", t.getHashAsString());
        InputStream input = new ByteArrayInputStream(t.unsafeBitcoinSerialize());
        String bucketName = "strongfellow.com";
        this.s3.putObject(bucketName, key, input, null);
        logger.info("finished stashing {}", t.getHashAsString());
    }
}
