package com.gsr.engine;

import com.gsr.analytics.Request;
import com.gsr.data.*;
import com.gsr.feed.ObjectPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OrderBookIntegrationTest {

    private OrderBookDistributor orderBookDistributor;
    private OrderBookProcessor btcBidProcessor;
    private OrderBookProcessor btcOfferProcessor;
    private OrderBookProcessor ethBidProcessor;
    private OrderBookProcessor ethOfferProcessor;
    private OrderBookProcessor solOfferProcessor;
    private OrderBookProcessor solBidProcessor;
    private ConcurrentLinkedQueue<Message> distributorInboundQueue;

    @Before
    public void setup(){
        ObjectPool<Message> messagePool = new ObjectPool<>(Message::new);
        distributorInboundQueue = new ConcurrentLinkedQueue<>();

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

        btcOfferProcessor = new OfferOrderBookProcessor(CcyPair.BTCUSD,   messagePool, queues.get(0), requestQueues.get(0), responseQueues.get(0)) ;
        btcBidProcessor = new BidOrderBookProcessor(CcyPair.BTCUSD,   messagePool, queues.get(1), requestQueues.get(1), responseQueues.get(1));
        ethBidProcessor = new BidOrderBookProcessor(CcyPair.ETHUSD,   messagePool, queues.get(2), requestQueues.get(2), responseQueues.get(2));
        ethOfferProcessor = new OfferOrderBookProcessor(CcyPair.ETHUSD,   messagePool, queues.get(3), requestQueues.get(3), responseQueues.get(3));
        solBidProcessor = new BidOrderBookProcessor(CcyPair.SOLUSD,   messagePool, queues.get(4), requestQueues.get(4), responseQueues.get(4) );
        solOfferProcessor = new OfferOrderBookProcessor(CcyPair.SOLUSD,   messagePool, queues.get(5), requestQueues.get(5), responseQueues.get(5));

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
    }


    @Test
    public void testSingleLimitVsMarketBid() throws InterruptedException {


    }


    private Message prepareMessage(CcyPair pair, Side side, MessageType type, long price, long quantity){
        Message message = new Message();
        message.setPair(pair);
        message.setSide(side);
        message.setType(type);
        message.setPrice(price);
        message.setQuantity(quantity);

        return message;
    }

    @After
    public void tearDown() {
        orderBookDistributor.shutdown();
        btcBidProcessor.shutdown();
        btcOfferProcessor.shutdown();
        ethBidProcessor.shutdown();
        ethOfferProcessor.shutdown();
    }
}