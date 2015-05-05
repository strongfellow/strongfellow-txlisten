
package com.strongfellow.txlisten;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.beust.jcommander.JCommander;
import com.strongfellow.utils.BlockParser;
import com.strongfellow.utils.ParseException;
import com.strongfellow.utils.data.Block;

public class BlockChainCrawler {

	private static final AmazonS3 s3 = new AmazonS3Client();
	private static final Logger logger = LoggerFactory.getLogger(BlockChainCrawler.class);
	
	public static void main(String[] args)
			throws AmazonServiceException, AmazonClientException, IOException, ParseException {
	
		BlockParser parser = new BlockParser();
		if (!"crawl".equals(args[0])) {
			throw new RuntimeException();
		}
		Args arg = new Args();
		new JCommander(arg, args);
		
		String bucket = arg.bucket;
		String block = arg.block;
		
		int count = 0;
		while (true) {
			String key = String.format("networks/f9beb4d9/blocks/%s/payload", block);
			byte[] bytes = IOUtils.toByteArray(s3.getObject(bucket, key).getObjectContent());
			Block blk = parser.parse(bytes);
			if (blk.getBlockHash().equals(block)) {
				logger.info("got {}", block);
			} else {
				logger.error("block hash does not match: {}, {}", block, blk.getBlockHash());
				System.exit(1);
			}
			block = blk.getHeader().getPreviousBlock();
			logger.info("count: {}", ++count);
		}
	}
}
