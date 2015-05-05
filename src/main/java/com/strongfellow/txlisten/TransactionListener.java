package com.strongfellow.txlisten;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.BlockChain;
import org.bitcoinj.core.GetDataMessage;
import org.bitcoinj.core.Message;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Peer;
import org.bitcoinj.core.PeerAddress;
import org.bitcoinj.core.PeerEventListener;
import org.bitcoinj.core.PeerGroup;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.BlockStoreException;
import org.bitcoinj.store.FullPrunedBlockStore;
import org.bitcoinj.store.H2FullPrunedBlockStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.strongfellow.utils.AmazonUtils.CacheException;
import com.strongfellow.utils.BlockParser;
import com.strongfellow.utils.ParseException;

public class TransactionListener {
	
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    protected static TransactionQueue transactionQueue = new TransactionQueue();
    private static final BlockStasher stasher = new BlockStasher();

    public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException, BlockStoreException {

    	Executor executor = Executors.newFixedThreadPool(4);
        final Map<String, Long> counters = new HashMap<String, Long>();
        counters.put("sqs::PUT", 0L);

        logger.info("begin");
        NetworkParameters params = MainNetParams.get();
    	final FullPrunedBlockStore blockStore = new H2FullPrunedBlockStore(params, "./blks", 100);
    	final BlockChain blockChain = new BlockChain(params, blockStore);
        PeerGroup peerGroup = new PeerGroup(params, blockChain);
        peerGroup.setUseLocalhostPeerWhenPossible(true);

        peerGroup.addEventListener(new PeerEventListener() {

            public void onTransaction(Peer peer, Transaction t) {
            	logger.info("not doing anything with {}", t.getHashAsString());
            	/**
            	logger.info("begin processing transaction {}", t.getHashAsString());
                try {
                    stasher.stash(t);
                    transactionQueue.put(t);
                    logger.info("finished processing transaction {}", t.getHashAsString());
                } catch (JsonProcessingException e) {
                    logger.error("couldn't fully process transaction", e);
                } catch (IOException e) {
                    logger.error("couldn't fully process transaction", e);
				}
				*/
            }

            public Message onPreMessageReceived(Peer peer, Message m) {
                return null;
            }

            public void onPeerDisconnected(Peer peer, int peerCount) {
                logger.info("peer disconnected: {}", peer.getAddress().toString());
            }

            public void onPeerConnected(Peer peer, int peerCount) {
                logger.info("peer connected: {}", peer.getAddress().toString());
            }

            public void onChainDownloadStarted(Peer peer, int blocksLeft) {
            	logger.info("chain download started with {} blocks left", blocksLeft);
            }

            public void onBlocksDownloaded(Peer peer, Block block, int blocksLeft) {
            	try {
            		final BlockParser blockParser = new BlockParser();
            		stasher.stash(Constants.MAIN_NETWORK, block);
				} catch (ParseException|CacheException e) {
					logger.error("couldn't stash block", e);
				}
            }

            public List<Message> getData(Peer peer, GetDataMessage m) {
                return null;
            }
        }, executor);

        peerGroup.start();
        peerGroup.addAddress(new PeerAddress(InetAddress.getLocalHost(), params.getPort()));
        peerGroup.waitForPeers(1).get();
        peerGroup.downloadBlockChain();

        logger.info("end");
    }

}
