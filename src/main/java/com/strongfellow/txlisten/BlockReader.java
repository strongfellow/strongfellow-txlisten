package com.strongfellow.txlisten;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.strongfellow.utils.BlockParser;
import com.strongfellow.utils.Main;
import com.strongfellow.utils.ParseException;
import com.strongfellow.utils.Utils;
import com.strongfellow.utils.data.Block;
import com.strongfellow.utils.data.Transaction;

public class BlockReader {
		
	private static final Logger logger = LoggerFactory.getLogger(BlockReader.class);
	
	public static void main(String[] args) throws IOException, ParseException, NoSuchAlgorithmException, InterruptedException {

		int txCount = 0;
		int blockCount = 0;
		
		final TransactionStasher stasher = new TransactionStasher();
		InputStream in = new BufferedInputStream(System.in);
		byte[] bytes = new byte[16 * 1024];
		
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
		BlockParser parser = new BlockParser();
		while (true) {
			int y = in.read(bytes, 0, 8);
			if (y == -1) {
				System.exit(0);
			}
			long n = Utils.uint32(bytes,  4);
			while (8 + n >= bytes.length) {
				byte[] newBs = new byte[2 * bytes.length];
				for (int i = 0; i < 8; i++) {
					newBs[i] = bytes[i];
				}
				bytes = newBs;
			}
			int location = 8;
			for (int i = 0; n != 0; i++) {
				int x = in.read(bytes, location, (int)n);
				n -= x;
				location += x;
				if (i != 0) {
					logger.info("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
					Thread.sleep(1000 * (1 << i));
				}
			}

			String bucketName = "strongfellow.com";
			Block block = parser.parse(bytes);
			String network = block.getMagicNumber();
			String key = String.format("networks/%s/blocks/%s/payload", network, block.getBlockHash());
			stasher.upload(bucketName, key, bytes);
			logger.info("put block no {}", ++blockCount);

			int offset = 88;
			int b = bytes[offset] & 0xff;
			if (b < 0xfd) {
				offset += 1;
			} else if (b == 0xfd) {
				offset += 3;
			} else if (b == 0xfe) {
				offset += 5;
			} else if (b == 0xff) {
				offset += 9;
			}
			for (Transaction t : block.getTransactions()) {
				int len = (int) t.getTxLength();
				String hash = t.getTxHash();
				String k = String.format("networks/%s/transactions/%s/payload", network, hash);
				String actualHash = Utils.doubleSha(bytes, offset, len);
				if (!actualHash.equals(hash)) {
					logger.info("expected hash: {}", hash);
					logger.info("actual hash: {}", actualHash);
					logger.info("exiting");
					System.exit(1);
				} else {
					logger.info("hash is good: {}", hash);
				}
				stasher.upload(bucketName, k, bytes, offset, len);
				offset += len;
				logger.info("put transaction no {}", ++txCount);
			}
		}
	}

}
