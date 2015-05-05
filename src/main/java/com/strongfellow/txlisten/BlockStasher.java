package com.strongfellow.txlisten;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.bitcoinj.core.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.strongfellow.utils.AmazonUtils;
import com.strongfellow.utils.AmazonUtils.CacheException;
import com.strongfellow.utils.ParseException;
import com.strongfellow.utils.AmazonUtils.CacheResult;

public class BlockStasher {
	
    private static final Logger logger = LoggerFactory.getLogger(BlockStasher.class);
    private final AmazonS3 s3 = new AmazonS3Client();
    private static final String BUCKET = "strongfellow.com";

    
    public CacheResult cache(String key, byte[] payload) throws CacheException {
    	return cache(key, payload, 0, payload.length);
    }

    public CacheResult cache(String key, byte[] payload, int offset, int len) throws CacheException {
    	return AmazonUtils.cache(s3,  BUCKET,  key,  payload, offset, len);
    }

    private static byte[] payload(String network, Block block) {
        byte[] blockBytes = block.bitcoinSerialize();
        byte[] payload = new byte[blockBytes.length + 8];


        for (int i = 0; i < 4; i++) {
        	payload[i] = (byte) Integer.parseInt(network.substring(2*i, 2*i+2), 16);
        }
        int len = blockBytes.length;
        for (int i = 4; len !=0; i++) {
        	payload[i] = (byte) (len & 0xff);
        	len >>= 8;
        }
        System.arraycopy(blockBytes, 0, payload, 8, blockBytes.length);
        return payload;
    }
    
    public void stash(String network, Block block) throws ParseException, CacheException {
    	logger.info("begin stashing block {}", block.getHashAsString());
        String key = String.format("networks/%s/blocks/%s/payload",
        		Constants.MAIN_NETWORK,
        		block.getHashAsString());
        byte[] payload = payload(network, block);
        cache(key, payload);
    	logger.info("finished stashing block {}", block.getHashAsString());
    }
}
    