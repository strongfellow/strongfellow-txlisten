package com.strongfellow.txlisten;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.BlockChain;
import org.bitcoinj.core.BlockChainListener;
import org.bitcoinj.core.FullPrunedBlockChain;
import org.bitcoinj.core.GetDataMessage;
import org.bitcoinj.core.Message;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Peer;
import org.bitcoinj.core.PeerAddress;
import org.bitcoinj.core.PeerEventListener;
import org.bitcoinj.core.PeerGroup;
import org.bitcoinj.core.ScriptException;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.VerificationException;
import org.bitcoinj.core.AbstractBlockChain.NewBlockType;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.BlockStore;
import org.bitcoinj.store.BlockStoreException;
import org.bitcoinj.store.FullPrunedBlockStore;
import org.bitcoinj.store.H2FullPrunedBlockStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.strongfellow.utils.Utils;

public class TransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    protected static TransactionQueue transactionQueue = new TransactionQueue();
    private static final TransactionStasher stasher = new TransactionStasher();

    public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException, BlockStoreException {

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
					stasher.stash(block);
				} catch (IOException e) {
					logger.error("couldn't stash block", e);
				}
            }

            public List<Message> getData(Peer peer, GetDataMessage m) {
                return null;
            }
        });

        peerGroup.start();
        peerGroup.addAddress(new PeerAddress(InetAddress.getLocalHost(), params.getPort()));
        peerGroup.waitForPeers(1).get();
        peerGroup.downloadBlockChain();

        logger.info("end");
    }

}
