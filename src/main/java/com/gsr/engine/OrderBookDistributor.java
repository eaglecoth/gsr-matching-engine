package com.gsr.engine;

import com.gsr.analytics.Request;
import com.gsr.data.CcyPair;
import com.gsr.data.Message;
import com.gsr.data.Side;
import com.gsr.feed.ObjectPool;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Class responsible for unpacking instructions and sending them for processing to the correct threads.
 * One thread running for each Currency pairs side of book.
 * Non blocking thread communication is provided via ConcurrentLinkedQueues.
 */
public class OrderBookDistributor {


    private final Map<CcyPair,Map<Side,ConcurrentLinkedQueue<Request>>> requestResponseQueues = new HashMap<>();
    private final Map<CcyPair,Map<Side,ConcurrentLinkedQueue<Request>>>   outboundRequestQueues = new HashMap<>();
    private final Map<CcyPair,Map<Side,ConcurrentLinkedQueue<Message>>>   outboundMdQueues = new HashMap<>();

    private final ConcurrentLinkedQueue<Request> incomingAnalyticsRequestQueue;
    private final ConcurrentLinkedQueue<Message> incomingMarketDataQueue;
    private final ConcurrentLinkedQueue<Request> analyticsResponseQueue;

    private volatile boolean runningFlag = true;

    private final ObjectPool<Message> messagePool;

    public OrderBookDistributor(ConcurrentLinkedQueue<Message> incomingMarketDataQueue,
                                ConcurrentLinkedQueue<Request> incomingAnalyticsRequests,
                                List<ConcurrentLinkedQueue<Message>> engineQueues,
                                List<ConcurrentLinkedQueue<Request>> requestQueues,
                                List<ConcurrentLinkedQueue<Request>> responseQueues,
                                ConcurrentLinkedQueue<Request> analyticsResponseQueue,
                                ObjectPool<Message> messagePool) {

        this.messagePool = messagePool;
        this.analyticsResponseQueue = analyticsResponseQueue;

        this.incomingMarketDataQueue = incomingMarketDataQueue;
        this.incomingAnalyticsRequestQueue = incomingAnalyticsRequests;

        Arrays.stream(CcyPair.values()).forEach(p -> {
            outboundRequestQueues.put(p, new HashMap<>());
            outboundMdQueues.put(p, new HashMap<>());
        });

        outboundMdQueues.get(CcyPair.BTCUSD).put(Side.Offer,engineQueues.get(0));
        outboundMdQueues.get(CcyPair.BTCUSD).put(Side.Bid,engineQueues.get(1));
        outboundMdQueues.get(CcyPair.ETHUSD).put(Side.Offer,engineQueues.get(2));
        outboundMdQueues.get(CcyPair.ETHUSD).put(Side.Bid,engineQueues.get(3));
        outboundMdQueues.get(CcyPair.SOLUSD).put(Side.Offer,engineQueues.get(4));
        outboundMdQueues.get(CcyPair.SOLUSD).put(Side.Bid,engineQueues.get(5));

        outboundRequestQueues.get(CcyPair.BTCUSD).put(Side.Offer,requestQueues.get(0));
        outboundRequestQueues.get(CcyPair.BTCUSD).put(Side.Bid,requestQueues.get(1));
        outboundRequestQueues.get(CcyPair.ETHUSD).put(Side.Offer,requestQueues.get(2));
        outboundRequestQueues.get(CcyPair.ETHUSD).put(Side.Bid,requestQueues.get(3));
        outboundRequestQueues.get(CcyPair.SOLUSD).put(Side.Offer,requestQueues.get(4));
        outboundRequestQueues.get(CcyPair.SOLUSD).put(Side.Bid,requestQueues.get(5));


        requestResponseQueues.get(CcyPair.BTCUSD).put(Side.Offer,responseQueues.get(0));
        requestResponseQueues.get(CcyPair.BTCUSD).put(Side.Bid,responseQueues.get(1));
        requestResponseQueues.get(CcyPair.ETHUSD).put(Side.Offer,responseQueues.get(2));
        requestResponseQueues.get(CcyPair.ETHUSD).put(Side.Bid,responseQueues.get(3));
        requestResponseQueues.get(CcyPair.SOLUSD).put(Side.Offer,responseQueues.get(4));
        requestResponseQueues.get(CcyPair.SOLUSD).put(Side.Bid,responseQueues.get(5));

        Thread mdThread = new Thread(() -> {
            System.out.println("Order Book Distributor Running");

            while (runningFlag) {
                Message message = this.incomingMarketDataQueue.poll();
                if (message != null) {
                    outboundMdQueues.get(message.getPair()).get(message.getSide()).add(message);
                }
            }
        }, "Market Data Distributor");

        Thread analyticsThread = new Thread(() -> {
            System.out.println("Analytics Request Distributor Running");

            while (runningFlag) {
                Request request = incomingAnalyticsRequestQueue.poll();
                if (request != null) {
                    outboundRequestQueues.get(request.getPair()).get(request.getSide()).add(request);
                }
            }
        }, "Analytics Request Distributor");


        Thread responseThread = new Thread(() -> {
            System.out.println("Analytics Response Collector Running");

            while (runningFlag) {
                for(Map<Side, ConcurrentLinkedQueue<Request>> map : requestResponseQueues.values()) {
                    for (ConcurrentLinkedQueue<Request> list : map.values()) {
                        Request request = list.poll();
                        if (request != null) {
                            analyticsResponseQueue.add(request);
                        }
                    }
                }
            }
        }, "Analytics Response Collector");

        mdThread.start();
        analyticsThread.start();
        responseThread.start();
    }

    public void shutdown() {
        System.out.println("Shuttingdown OrderBook Distributor");
        runningFlag = false;
    }
}
