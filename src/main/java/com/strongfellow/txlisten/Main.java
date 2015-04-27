package com.strongfellow.txlisten;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.GetDataMessage;
import org.bitcoinj.core.Message;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Peer;
import org.bitcoinj.core.PeerAddress;
import org.bitcoinj.core.PeerEventListener;
import org.bitcoinj.core.PeerGroup;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.params.MainNetParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    protected static TransactionQueue transactionQueue = new TransactionQueue();
    private static final TransactionStasher stasher = new TransactionStasher();

    public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException {

        final Map<String, Long> counters = new HashMap<String, Long>();
        counters.put("sqs::PUT", 0L);


        logger.info("begin");
        NetworkParameters params = MainNetParams.get();
        PeerGroup peerGroup = new PeerGroup(params);
        peerGroup.setUseLocalhostPeerWhenPossible(true);

        peerGroup.addEventListener(new PeerEventListener() {

            public void onTransaction(Peer peer, Transaction t) {
                logger.info("begin processing transaction {}", t.getHashAsString());
                stasher.stash(t);
                try {
                    transactionQueue.put(t);
                } catch (JsonProcessingException e) {
                    logger.error("couldn't fully process transaction", e);
                }
                logger.info("finished processing transaction {}", t.getHashAsString());

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
            }

            public void onBlocksDownloaded(Peer peer, Block block, int blocksLeft) {
            }

            public List<Message> getData(Peer peer, GetDataMessage m) {
                return null;
            }
        });

        peerGroup.start();
        peerGroup.addAddress(new PeerAddress(InetAddress.getLocalHost(), params.getPort()));
        peerGroup.waitForPeers(1).get();

        logger.info("end");
    }

}
