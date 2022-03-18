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
    private ConcurrentLinkedQueue<Message> distributorMdQueue;
    private ConcurrentLinkedQueue<Request> analyticsRequestQueue;
    private List<String> testMarketData;
    private MessageSerializer serializer;
    private FileLoader fileLoader;
    private final double TEST_ACCEPTANCE_DELTA = 0.000000000001;


    //Why a blocking queue here?  Well in a prod scenario something else would make sense -- But this is just a test
    //class.  Nice and easy to block in wait for test results to arrive
    private LinkedBlockingQueue<Request> analyticsResponseQueue;



    //Adjust this timeout to suit your own hardware. If you want to block -- Implement your own busy wait.
    private final long WAIT_TIME = 100;

    @Before
    public void setup(){
        ObjectPool<Message> messagePool = new ObjectPool<>(Message::new);
        distributorMdQueue = new ConcurrentLinkedQueue<>();
        analyticsRequestQueue = new ConcurrentLinkedQueue<>();
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

        //Launch the order book threads
        btcOfferProcessor.launchOrderBookThread();
        btcBidProcessor.launchOrderBookThread();
        ethOfferProcessor.launchOrderBookThread();
        ethBidProcessor.launchOrderBookThread();
        solOfferProcessor.launchOrderBookThread();
        solBidProcessor.launchOrderBookThread();


        //Load the messages from file
        fileLoader = new FileLoader();
        serializer = new MessageSerializerImpl(distributorMdQueue, messagePool, 3, 100, MESSAGE_DELIMITER, KEY_VALUE_DELIMITER);

        //Store test data in list
        testMarketData = fileLoader.readFileEntries("/testUpdates.csv");
    }


    @Test
    public void testNoCrosses() throws InterruptedException {

        //Take a line from the MD csv file and send to the serializer, which then sends the request on to the
        //distributor, which will identify the correct order book. We add a sleep here to make sure that the
        //market data is processed before we send any request for analytics. We can't dance around this if we want to
        //avoid expensive blocking operations. Which we want.

        serializer.onMessage(testMarketData.get(1));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(1,CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 1));
        Request request = analyticsResponseQueue.take();
        assertEquals( 1,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(2));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(2, CcyPair.BTCUSD, Side.Offer, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 2,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(3));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(3,CcyPair.SOLUSD, Side.Bid, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 3,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(4));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(4, CcyPair.SOLUSD, Side.Offer, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 4,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(5));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(5, CcyPair.ETHUSD, Side.Bid, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 5.12,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(6));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(6, CcyPair.ETHUSD, Side.Offer, RequestType.AveragePrice, 1));
        request = analyticsResponseQueue.take();
        assertEquals( 6.2,request.getResult(), TEST_ACCEPTANCE_DELTA);

    }


    @Test
    public void testTopOfBook() throws InterruptedException {

        //Check top of book is correctly updated
        serializer.onMessage(testMarketData.get(1));
        Thread.sleep(WAIT_TIME);
        assertEquals( 1,btcBidProcessor.getTopOfBookPrice(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(2));
        Thread.sleep(WAIT_TIME);
        assertEquals( 2,btcOfferProcessor.getTopOfBookPrice(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testAveragePriceCalculationsBid() throws InterruptedException {

        //Check average prices are correclty calculated
        serializer.onMessage(testMarketData.get(8));
        serializer.onMessage(testMarketData.get(9));
        serializer.onMessage(testMarketData.get(10));
        Thread.sleep(WAIT_TIME);

        analyticsRequestQueue.add(createAnalyticsRequest(7, CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 3.0,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }

    @Test
    public void testAveragePriceCalculationsOffer() throws InterruptedException {


        serializer.onMessage(testMarketData.get(12));
        serializer.onMessage(testMarketData.get(13));
        serializer.onMessage(testMarketData.get(14));
        Thread.sleep(WAIT_TIME);

        analyticsRequestQueue.add(createAnalyticsRequest(8, CcyPair.BTCUSD, Side.Offer, RequestType.AveragePrice, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 10.0,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testQuantityOverLevelsBid() throws InterruptedException {

        //Check quantities are correctly calculated
        serializer.onMessage(testMarketData.get(8));
        serializer.onMessage(testMarketData.get(9));
        serializer.onMessage(testMarketData.get(10));
        Thread.sleep(WAIT_TIME);

        analyticsRequestQueue.add(createAnalyticsRequest(9, CcyPair.BTCUSD, Side.Bid, RequestType.AverageQuantity, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 40.0,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testQuantityOverLevelsOffer() throws InterruptedException {

        serializer.onMessage(testMarketData.get(12));
        serializer.onMessage(testMarketData.get(13));
        serializer.onMessage(testMarketData.get(14));
        Thread.sleep(WAIT_TIME);

        analyticsRequestQueue.add(createAnalyticsRequest(10, CcyPair.BTCUSD, Side.Offer, RequestType.AverageQuantity, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 200.0,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testVwapLevelsBid() throws InterruptedException {

        serializer.onMessage(testMarketData.get(8));
        serializer.onMessage(testMarketData.get(9));
        serializer.onMessage(testMarketData.get(10));
        Thread.sleep(WAIT_TIME);

        analyticsRequestQueue.add(createAnalyticsRequest(11, CcyPair.BTCUSD, Side.Bid, RequestType.Vwap, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 3.75,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }

    @Test
    public void testVwapLevelsOffer() throws InterruptedException {

        serializer.onMessage(testMarketData.get(12));
        serializer.onMessage(testMarketData.get(13));
        serializer.onMessage(testMarketData.get(14));
        Thread.sleep(WAIT_TIME);

        analyticsRequestQueue.add(createAnalyticsRequest(12, CcyPair.BTCUSD, Side.Offer, RequestType.Vwap, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 10.4,request.getResult(), TEST_ACCEPTANCE_DELTA);
    }


    @Test
    public void testQuantityUpdateBid() throws InterruptedException {


        serializer.onMessage(testMarketData.get(16));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(13, CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 8.0,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(17));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(14, CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 3));
        request = analyticsResponseQueue.take();
        assertEquals( 8.5,request.getResult(), TEST_ACCEPTANCE_DELTA);


        serializer.onMessage(testMarketData.get(18));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(15, CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 3));
        request = analyticsResponseQueue.take();
        assertEquals( 9.0,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(19));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(16, CcyPair.BTCUSD, Side.Bid, RequestType.AveragePrice, 3));
        request = analyticsResponseQueue.take();
        assertEquals( 0,request.getResult(), TEST_ACCEPTANCE_DELTA);

    }

    @Test
    public void testQuantityUpdateOffer() throws InterruptedException {

        serializer.onMessage(testMarketData.get(21));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(17, CcyPair.BTCUSD, Side.Offer, RequestType.AveragePrice, 3));
        Request request = analyticsResponseQueue.take();
        assertEquals( 9.0,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(22));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(18, CcyPair.BTCUSD, Side.Offer, RequestType.AveragePrice, 3));
        request = analyticsResponseQueue.take();
        assertEquals( 8.5,request.getResult(), TEST_ACCEPTANCE_DELTA);


        serializer.onMessage(testMarketData.get(23));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(19, CcyPair.BTCUSD, Side.Offer, RequestType.AveragePrice, 3));
        request = analyticsResponseQueue.take();
        assertEquals( 8,request.getResult(), TEST_ACCEPTANCE_DELTA);

        serializer.onMessage(testMarketData.get(24));
        Thread.sleep(WAIT_TIME);
        analyticsRequestQueue.add(createAnalyticsRequest(20, CcyPair.BTCUSD, Side.Offer, RequestType.AveragePrice, 3));
        request = analyticsResponseQueue.take();
        assertEquals( 0,request.getResult(), TEST_ACCEPTANCE_DELTA);

    }

    private Request createAnalyticsRequest(int id, CcyPair pair, Side side, RequestType type, int levels) {
        return new Request(id, levels, type, side, pair);
    }

    @After
    public void tearDown() {
        orderBookDistributor.shutdown();
        btcBidProcessor.shutDownOrderBookThread();
        btcOfferProcessor.shutDownOrderBookThread();
        ethBidProcessor.shutDownOrderBookThread();
        ethOfferProcessor.shutDownOrderBookThread();
    }
}