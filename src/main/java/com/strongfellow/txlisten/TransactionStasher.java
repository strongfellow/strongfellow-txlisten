package com.strongfellow.txlisten;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.strongfellow.utils.Utils;

public class TransactionStasher {

    private static final Logger logger = LoggerFactory.getLogger(TransactionStasher.class);
    private final AmazonS3 s3 = new AmazonS3Client();

    public void stash(Transaction t) throws IOException {
        logger.info("begin stashing transaction {}", t.getHashAsString());
        String bucketName = "strongfellow.com";
        String key = String.format("transactions/%s/payload", t.getHashAsString());
        byte[] payload = t.bitcoinSerialize();
        upload(bucketName, key, payload);
        logger.info("finished stashing transaction {}", t.getHashAsString());
    }
    
    public void stash(Block block) throws IOException {
        logger.info("begin stashing block {}", block.getHashAsString());
        String bucketName = "strongfellow.com";
        String key = String.format("blocks/%s/payload", block.getHashAsString());
        byte[] payload = block.unsafeBitcoinSerialize();
        upload(bucketName, key, payload);
        logger.info("finished stashing block {}", block.getHashAsString());
    }
    
    public void upload(String bucketName, String key, byte[] bytes) throws IOException {
    	upload(bucketName, key, bytes, 0, bytes.length);
    }
    
    void upload(String bucketName, String key, byte[] payload, int offset, int length) throws IOException {
    	boolean cacheHit = false;
    	try {
    		ObjectMetadata meta = this.s3.getObjectMetadata(bucketName, key);
    		String etag = meta.getETag();
    		logger.info("etag in s3: {}", etag);
    		MessageDigest digest = MessageDigest.getInstance("MD5");
    		digest.update(payload, offset, length);
    		String md5 = Utils.hex(digest.digest());
    		logger.info("md5 locally: {}", md5);
    		if (md5 !=  null && md5.equals(etag)) {
    			cacheHit = true;
    		}
    	} catch(Throwable t) {
    		logger.info("couldn't find {}", key);
    	}
    	
    	if (cacheHit) {
        	logger.info("cache hit, skipping block");
        } else {
        	logger.info("cache miss, uploading block");
        	InputStream input = new ByteArrayInputStream(payload, offset, length);
        	this.s3.putObject(bucketName, key, input, null);
        	input.close();
        }
    }
}
