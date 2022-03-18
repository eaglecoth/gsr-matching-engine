package com.gsr.engine;

import com.gsr.analytics.Request;
import com.gsr.analytics.RequestType;
import com.gsr.application.FileLoader;
import com.gsr.data.*;
import com.gsr.feed.MessageSerializer;
import com.gsr.feed.MessageSerializerImpl;
import com.gsr.feed.ObjectPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.gsr.data.Constants.KEY_VALUE_DELIMITER;
import static com.gsr.data.Constants.MESSAGE_DELIMITER;
import static org.junit.Assert.assertEquals;

public class OrderBookIntegrationTest {

    private OrderBookDistributor orderBookDistributor;
    private OrderBookProcessor btcBidProcessor;
    private OrderBookProcessor btcOfferProcessor;
    private OrderBookProcessor ethBidProcessor;
    private OrderBookProcessor ethOfferProcessor;
    private OrderBookProcessor solOfferProcessor;
    private OrderBookProcessor solBidProcessor;
    private LinkedBlockingQueue<Message> distributorMdQueue;
    private LinkedBlockingQueue<Request> analyticsResponseQueue;
    private LinkedBlockingQueue<Request> analyticsRequestQueue;
    private List<String> testMarketData;
    private MessageSerializer serializer;
    private FileLoader fileLoader;

    private double TEST_ACCEPTANCE_DELTA = 0.000000000001;


    @Before
    public void setup(){
        ObjectPool<Message> messagePool = new ObjectPool<>(Message::new);
        distributorMdQueue = new LinkedBlockingQueue<>();
        analyticsRequestQueue = new LinkedBlockingQueue<>();
        analyticsResponseQueue = new LinkedBlockingQueue<>();




        List<ConcurrentLinkedQueue<Message>> queues = new ArrayList<>(6);
        for (int i = 0; i < 6; i++) {
            queues.add(new ConcurrentLinkedQueue<>());
        }

        List<ConcurrentLinkedQueue<Request>> requestQueues = new ArrayList<>(6);
        for (int i = 0; i < 6; i++) {
            requestQueues.add(new ConcurrentLinkedQueue<>());
        }

        List<ConcurrentLinkedQueue<Request>> responseQueues = new ArrayList<>(6);
        for (int i = 0; i < 6; i++) {
            responseQueues.add(new ConcurrentLinkedQueue<>());
        }

        orderBookDistributor = new OrderBookDistributor(distributorMdQueue, analyticsRequestQueue, queues, requestQueues, responseQueues, analyticsResponseQueue);
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


        //Load the messages from file
        fileLoader = new FileLoader();
        serializer = new MessageSerializerImpl(distributorMdQueue, messagePool, 3, 100, MESSAGE_DELIMITER, KEY_VALUE_DELIMITER);

        testMarketData = fileLoader.readFileEntries("/testUpdates.csv");
    }


    @Test
    public void testNoCrosses() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(1));
        Thread.sleep(timeout);
        analyticsRequestQueue.add(createAnalyticsRequest(1,CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 1));
        Request request = analyticsResponseQueue.take();
        assertEquals( 1,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(2));
        Thread.sleep(timeout);
        analyticsRequestQueue.add(createAnalyticsRequest(2, CcyPair.BTCUSD, Side.Offer, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 2,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(3));
        Thread.sleep(timeout);
        analyticsRequestQueue.add(createAnalyticsRequest(3,CcyPair.SOLUSD, Side.Bid, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 3,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(4));
        Thread.sleep(timeout);
        analyticsRequestQueue.add(createAnalyticsRequest(4, CcyPair.SOLUSD, Side.Offer, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 4,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(5));
        Thread.sleep(timeout);
        analyticsRequestQueue.add(createAnalyticsRequest(5, CcyPair.ETHUSD, Side.Bid, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 5.12,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(6));
        Thread.sleep(timeout);
        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.ETHUSD, Side.Offer, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 6.2,request.getResult(), TEST_ACCEPTANCE_DELTA);

    }


    @Test
    public void testTopOfBook() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(1));
        Thread.sleep(timeout);
        assertEquals( 1,btcBidProcessor.getTopOfBookPrice(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(2));
        Thread.sleep(timeout);
        assertEquals( 2,btcOfferProcessor.getTopOfBookPrice(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testAveragePriceCalculationsBid() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(8));
        serializer.onMessage(testMarketData.get(9));
        serializer.onMessage(testMarketData.get(10));
        Thread.sleep(timeout);

        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 3.0,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }

    @Test
    public void testAveragePriceCalculationsOffer() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(12));
        serializer.onMessage(testMarketData.get(13));
        serializer.onMessage(testMarketData.get(14));
        Thread.sleep(timeout);

        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.BTCUSD, Side.Offer, RequestType.AveragePrice, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 10.0,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testQuantityOverLevelsBid() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(8));
        serializer.onMessage(testMarketData.get(9));
        serializer.onMessage(testMarketData.get(10));
        Thread.sleep(timeout);

        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.BTCUSD, Side.Bid, RequestType.AverageQuantity, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 40.0,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testQuantityOverLevelsOffer() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(12));
        serializer.onMessage(testMarketData.get(13));
        serializer.onMessage(testMarketData.get(14));
        Thread.sleep(timeout);

        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.BTCUSD, Side.Offer, RequestType.AverageQuantity, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 200.0,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testVwapLevelsBid() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(8));
        serializer.onMessage(testMarketData.get(9));
        serializer.onMessage(testMarketData.get(10));
        Thread.sleep(timeout);

        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.BTCUSD, Side.Bid, RequestType.Vwap, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 3.75,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }

    @Test
    public void testVwapLevelsOffer() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(12));
        serializer.onMessage(testMarketData.get(13));
        serializer.onMessage(testMarketData.get(14));
        Thread.sleep(timeout);

        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.BTCUSD, Side.Offer, RequestType.Vwap, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 10.4,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testQuantityUpdate() throws InterruptedException {

        long timeout = 100;

        serializer.onMessage(testMarketData.get(16));
        Thread.sleep(timeout);

        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 3.0,request.getResult(), TEST_ACCEPTANCE_DELTA);


        serializer.onMessage(testMarketData.get(9));
        serializer.onMessage(testMarketData.get(10));



    }






    private Request createAnalyticsRequest(int id, CcyPair pair, Side side, RequestType type, int levels) {
        return new Request(id, levels, type, side, pair);
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