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


import static com.gsr.data.Constants.*;


/**
 * Main running class to test the matching engine. Some sample orders being
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

        OrderBookDistributor orderBookDistributor = new OrderBookDistributor(distributorInboundQueue, queues, messagePool);


        OrderBookProcessor btcOfferProcessor = new OfferOrderBookProcessor(CcyPair.BTCUSD,   messagePool, queues.get(0), requestQueues.get(0), responseQueues.get(0)) ;
        OrderBookProcessor btcBidProcessor = new BidOrderBookProcessor(CcyPair.BTCUSD,   messagePool, queues.get(1), requestQueues.get(1), responseQueues.get(1));
        OrderBookProcessor ethBidProcessor = new BidOrderBookProcessor(CcyPair.ETHUSD,   messagePool, queues.get(2), requestQueues.get(2), responseQueues.get(2));
        OrderBookProcessor ethOfferProcessor = new OfferOrderBookProcessor(CcyPair.ETHUSD,   messagePool, queues.get(3), requestQueues.get(3), responseQueues.get(3));
        OrderBookProcessor solBidProcessor = new BidOrderBookProcessor(CcyPair.SOLUSD,   messagePool, queues.get(4), requestQueues.get(4), responseQueues.get(4) );
        OrderBookProcessor solOfferProcessor = new OfferOrderBookProcessor(CcyPair.SOLUSD,   messagePool, queues.get(5), requestQueues.get(5), responseQueues.get(5));

        btcOfferProcessor.setCorrespondingBook(btcBidProcessor);
        btcBidProcessor.setCorrespondingBook(btcOfferProcessor);
        ethOfferProcessor.setCorrespondingBook(ethBidProcessor);
        ethBidProcessor.setCorrespondingBook(ethOfferProcessor);
        solOfferProcessor.setCorrespondingBook(solBidProcessor);
        solBidProcessor.setCorrespondingBook(solOfferProcessor);


        btcOfferProcessor.startOrderBook();
        btcBidProcessor.startOrderBook();
        ethOfferProcessor.startOrderBook();
        ethBidProcessor.startOrderBook();
        solOfferProcessor.startOrderBook();
        solBidProcessor.startOrderBook();



        //Load the messages from file, send to the engine via the serializer
        FileLoader fileLoader = new FileLoader();

        fileLoader.readFileEntries(args[0]).forEach(serializer::onMessage);

        Thread.sleep(200);


        orderBookDistributor.shutdown();
        btcBidProcessor.shutdown();
        btcOfferProcessor.shutdown();
        ethBidProcessor.shutdown();
        ethOfferProcessor.shutdown();
    }
}
