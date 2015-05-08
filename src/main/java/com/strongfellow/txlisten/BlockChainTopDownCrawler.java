package com.strongfellow.txlisten;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.GetDataMessage;
import org.bitcoinj.core.Message;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Peer;
import org.bitcoinj.core.PeerEventListener;
import org.bitcoinj.core.PeerGroup;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.params.MainNetParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.ListenableFuture;
import com.strongfellow.utils.ParseException;

public class BlockChainTopDownCrawler {

	private static final Logger logger = LoggerFactory.getLogger(BlockChainTopDownCrawler.class);
	
	public static class Args {
		
		@Parameter
		public List<String> commands;
		
		@Parameter(names = {"--bucket"})
		public String bucket;
		
		@Parameter(names = {"--block"})
		public String block;
	}
	
	public static void main(String[] a) throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException, ParseException {
        NetworkParameters params = MainNetParams.get();
        PeerGroup peerGroup = new PeerGroup(params);
        peerGroup.setUseLocalhostPeerWhenPossible(true);
        peerGroup.startAndWait();

        Thread.sleep(7000);
		Peer peer = peerGroup.connectToLocalHost();
		
		BlockStasher blockStasher = new BlockStasher();
		Args args = new Args();
		new JCommander(args, a);
		String block = args.block;
		Sha256Hash blockHash = new Sha256Hash(block);
		int success = 0;
		while(true) {
			try {
				logger.info("getting {}", blockHash);
				ListenableFuture<Block> future = peer.getBlock(blockHash);
				Block blk = future.get();
				blockStasher.stash(Constants.MAIN_NETWORK, blk);
				blockHash = blk.getPrevBlockHash();
				logger.info("we have successfully stashed {} blocks", ++success);
			} catch(Throwable t) {
				logger.error("problem crawling: " + t.getMessage(), t);
				peerGroup.stopAndWait();
		        peerGroup = new PeerGroup(params);
		        peerGroup.setUseLocalhostPeerWhenPossible(true);
		        peerGroup.startAndWait();
				peer = peerGroup.connectToLocalHost();
			}
		}
	}
}
