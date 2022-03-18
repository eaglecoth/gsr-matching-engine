package com.gsr.application;

import com.gsr.analytics.Request;
import com.gsr.data.CcyPair;

import com.gsr.data.Message;
import com.gsr.engine.*;
import com.gsr.feed.ObjectPool;
import com.gsr.feed.MessageSerializer;
import com.gsr.feed.MessageSerializerImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;


import static com.gsr.data.Constants.*;


/**
 * Sample class to run the order book replicator. Its not particularly useful. Prefer using OrderBookIntegrationTest
 * instead.
 */
public class OrderBookReplicatorRunner {

    public static void main(String[] args) throws InterruptedException {

        List<ConcurrentLinkedQueue<Message>> queues = new ArrayList<>(6);
        for (int i = 0; i < 6; i++) {
            queues.add(new ConcurrentLinkedQueue<>());
        }

        List<ConcurrentLinkedQueue<Request>> requestQueues = new ArrayList<>(6);
        for (int i = 0; i < 6; i++) {
            queues.add(new ConcurrentLinkedQueue<>());
        }

        List<ConcurrentLinkedQueue<Request>> responseQueues = new ArrayList<>(6);
        for (int i = 0; i < 6; i++) {
            queues.add(new ConcurrentLinkedQueue<>());
        }

        ObjectPool<Message> messagePool = new ObjectPool<>(Message::new);

        ConcurrentLinkedQueue<Message> distributorInboundQueue = new ConcurrentLinkedQueue<>();
        MessageSerializer serializer = new MessageSerializerImpl(distributorInboundQueue, messagePool, 3, 100, MESSAGE_DELIMITER, KEY_VALUE_DELIMITER);

        ConcurrentLinkedQueue<Request> analyticsRequestQueue = new ConcurrentLinkedQueue<>();
        LinkedBlockingQueue<Request>  analyticsResponseQueue = new LinkedBlockingQueue<>();
        OrderBookDistributor orderBookDistributor = new OrderBookDistributor(distributorInboundQueue, analyticsRequestQueue, queues, requestQueues, responseQueues, analyticsResponseQueue);


        //Configure instances for each pair and side
        OrderBookProcessor btcOfferProcessor = new OfferOrderBookProcessor(CcyPair.BTCUSD,   messagePool, queues.get(0), requestQueues.get(0), responseQueues.get(0)) ;
        OrderBookProcessor btcBidProcessor = new BidOrderBookProcessor(CcyPair.BTCUSD,   messagePool, queues.get(1), requestQueues.get(1), responseQueues.get(1));
        OrderBookProcessor ethBidProcessor = new BidOrderBookProcessor(CcyPair.ETHUSD,   messagePool, queues.get(2), requestQueues.get(2), responseQueues.get(2));
        OrderBookProcessor ethOfferProcessor = new OfferOrderBookProcessor(CcyPair.ETHUSD,   messagePool, queues.get(3), requestQueues.get(3), responseQueues.get(3));
        OrderBookProcessor solBidProcessor = new BidOrderBookProcessor(CcyPair.SOLUSD,   messagePool, queues.get(4), requestQueues.get(4), responseQueues.get(4) );
        OrderBookProcessor solOfferProcessor = new OfferOrderBookProcessor(CcyPair.SOLUSD,   messagePool, queues.get(5), requestQueues.get(5), responseQueues.get(5));


        //Start the treads for each of the order book sides
        btcOfferProcessor.launchOrderBookThread();
        btcBidProcessor.launchOrderBookThread();
        ethOfferProcessor.launchOrderBookThread();
        ethBidProcessor.launchOrderBookThread();
        solOfferProcessor.launchOrderBookThread();
        solBidProcessor.launchOrderBookThread();


        //Load the messages from file, send to the engine via the serializer
        FileLoader fileLoader = new FileLoader();

        fileLoader.readFileEntries(args[0]).forEach(serializer::onMessage);

        Thread.sleep(200);


        orderBookDistributor.shutdown();
        btcBidProcessor.shutDownOrderBookThread();
        btcOfferProcessor.shutDownOrderBookThread();
        ethBidProcessor.shutDownOrderBookThread();
        ethOfferProcessor.shutDownOrderBookThread();
    }
}
