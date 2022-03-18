package com.gsr.engine;

import com.gsr.analytics.Request;
import com.gsr.data.*;
import com.gsr.feed.ObjectPool;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The OrderBookProcessor is an instance to represent and manage one side of a book for a particular currency pair
 * It holds price levels in a pseudo linked list
 */
public abstract class OrderBookProcessor {
    private Thread engineThread;
    private final CcyPair ccyPair;

    private final int MAX_PENDING_MD_UPDATES = 100;
    private final int MAX_PENDING_ANALYTICS_REQ = 100;
    private final int MAX_WAIT_NANOS = 20000;

    protected volatile boolean runningFlag;
    protected final TreeMap<Long, PriceLevel> orderBookPriceIndex = new TreeMap<>();
    protected final ObjectPool<Message> messageObjectPool;
    private final ObjectPool<PriceLevel> priceLevelObjectPool;
    protected AtomicReference<PriceLevel> topOfBook;

    private final Map<Integer, Long> quantityCalculationsCache = new HashMap<>();
    private final Map<Integer, Double> vwapCalculationsCache = new HashMap<>();
    private final Map<Integer, Double> priceCalculationsCache = new HashMap<>();

    public OrderBookProcessor(CcyPair ccyPair,
                              ObjectPool<Message> messageObjectPool,
                              ConcurrentLinkedQueue<Message> marketDataInboundQueue,
                              ConcurrentLinkedQueue<Request> analyticsRequestQueue,
                              ConcurrentLinkedQueue<Request> analyticsResponseQueue) {

        this.ccyPair = ccyPair;
        this.messageObjectPool = messageObjectPool;
        this.priceLevelObjectPool = new ObjectPool<>(PriceLevel::new);
        this.topOfBook = new AtomicReference<>(null);

        configureOrderBookThread(marketDataInboundQueue, analyticsRequestQueue, analyticsResponseQueue);
    }

    /**
     * Main method to configure the order book thread and its main execution task.
     *
     * The thread will read both market data updates, and service requests for analytics.  How these two conflicting
     * tasks are balanced is ultimately up to performance considerations.  The key feature here is that no
     * synchronization is required at any time which means 100% cpu utilisation a 100% of the time. Just pin each
     * of these threads to a specific CPU core.
     *
     *
     * @param inboundMdQueue        Queue of inbound market data information
     * @param analyticsRequestQueue Queue of inbound analytics requests
     * @param outboundResultQueue   Queue of outbound responses to analytics requests
     */
    private void configureOrderBookThread(final ConcurrentLinkedQueue<Message> inboundMdQueue, final ConcurrentLinkedQueue<Request> analyticsRequestQueue, final ConcurrentLinkedQueue<Request> outboundResultQueue) {
        engineThread = new Thread(() -> {
            System.out.println("Order Book Processor on ccy: [" + ccyPair + "] on side: [" + getSide() + "] started.");

            long lastMdUpdate = Long.MAX_VALUE;
            long lastService = Long.MAX_VALUE;

            while (runningFlag) {

                boolean hasUpdatedBook = false;

                Message marketDataMessage = inboundMdQueue.poll();

                while (marketDataMessage != null) {
                    processMessage(marketDataMessage);
                    hasUpdatedBook = true;
                    lastMdUpdate = System.nanoTime();

                    if (analyticsRequestQueue.size() > MAX_PENDING_ANALYTICS_REQ || isTimeUp(lastService, analyticsRequestQueue)) {
                        //If we pooled up too many analytics requests, or we have pending requests, but we've been executing
                        //for too long, we must give up on the incoming MD and service the analytics requests.
                        break;
                    }
                    marketDataMessage = inboundMdQueue.poll();
                }

                if (hasUpdatedBook) {
                    //The cached analytics results are now no longer correct and must be removed
                    clearCalculationResultCache();
                }

                Request request = analyticsRequestQueue.poll();

                while (request != null){

                    switch (request.getType()) {

                        case Vwap:
                            request.populateResult(vwapCalculationsCache.computeIfAbsent(request.getLevels(), this::calculateVwapOverLevels));
                            break;

                        case AveragePrice:
                            request.populateResult(priceCalculationsCache.computeIfAbsent(request.getLevels(), this::calculateAveragePrice));
                            break;

                        case AverageQuantity:
                            request.populateResult(quantityCalculationsCache.computeIfAbsent(request.getLevels(), this::calculateAccumulatedQuantityOverLevels));
                            break;
                    }
                    outboundResultQueue.add(request);
                    lastService = System.nanoTime();
                    if (inboundMdQueue.size() >= MAX_PENDING_MD_UPDATES || isTimeUp(lastMdUpdate, inboundMdQueue)) {
                        //If we pooled up too many md updates, or we have pending requests, but we've been executing
                        //for too long, we must give up on the analytics requests and update the book with new MD.
                        break;
                    }
                    request = analyticsRequestQueue.poll();
                }
            }
        }, "OrderBook-" + ccyPair + "-" + getSide());
    }

    public void launchOrderBookThread() {
        runningFlag = true;
        engineThread.start();
    }

    public void shutDownOrderBookThread() {
        System.out.println("Order Book Processor on ccy: [" + ccyPair + "] on side: [" + getSide() + "] shutting down.");
        runningFlag = false;
    }

    /**
     * Main processing method.  Incoming messages are categorised by type and processed accordingly. All processing
     * within the book itself happens synchronously.
     *
     * @param message message to be processed by the orderbook
     */
    private void processMessage(Message message) {

        switch (message.getType()) {
            case RemovePriceLevel:
                PriceLevel levelToRemove = orderBookPriceIndex.remove(message.getPrice());
                if (levelToRemove != null && levelToRemove.removePriceFromBook()) {
                    //The price level we removed was the last one in the book. Hence top of book should be set to null
                    topOfBook.set(null);
                }
                System.out.println("Removed from book: [" + message.getPair() + "] side: [" + message.getSide() + "] price: [" + message.getPrice() + "]");
                messageObjectPool.returnObject(message);
                return;

            case AddOrUpdatePriceLevel:
                addOrUpdatePriceLevel(message);
                System.out.println("Added to book: [" + message.getPair() + "] side: [" + message.getSide() + "] price: [" + message.getPrice() + "] Quantity: [" + message.getQuantity() + "]");
                messageObjectPool.returnObject(message);
        }

    }

    /**
     * Helper method to add or update quantity at a price, and if such price does not exist
     * then create it
     *
     * @param message containing a quantity which is to be inserted into the book.
     */
    private void addOrUpdatePriceLevel(final Message message) {

        orderBookPriceIndex.compute(message.getPrice(), (priceLevel, limit) -> {

            //If this is the first order of this price create the new limit book
            if (limit == null) {
                limit = addNewPriceLevelToBook(priceLevel, message.getQuantity());
            } else {
                limit.adjustQuantity(message.getQuantity());
            }
            return limit;
        });
    }

    /**
     * Helper method to acquire a new object from pool and insert in to chain of price levels
     *
     * @param price the price for which no current orders exist
     * @return the newly added price limit
     */
    private PriceLevel addNewPriceLevelToBook(Long price, long quantity) {
        PriceLevel priceLevel = priceLevelObjectPool.acquireObject();
        priceLevel.populate(price, quantity);

        //Unless this is the first price of this book traverse chain and insert.
        //This is not thread safe, but it needs not to be as only one thread ever will make modifications
        //on the book.
        if (!topOfBook.compareAndSet(null, priceLevel)) {
            insertPriceInBook(priceLevel, topOfBook.get());
        }
        return priceLevel;
    }


    /**
     *
     * @param lastService last time we serviced a request from the queue
     * @param queue the queue in question
     * @return true if we have been executing for too long and the queue has pending messages
     */
    private boolean isTimeUp(long lastService, Queue<?> queue) {
        return !queue.isEmpty() && System.nanoTime() - lastService > MAX_WAIT_NANOS;
    }

    /**
     * Helper method to clear cache
     */
    private void clearCalculationResultCache() {
        priceCalculationsCache.clear();
        vwapCalculationsCache.clear();
        quantityCalculationsCache.clear();
    }

    //Methods to be implemented depending on side

    abstract void insertPriceInBook(PriceLevel newPriceLevel, PriceLevel currentPriceLevel);

    protected abstract Side getSide();

    public abstract double calculateAveragePrice(int levels);

    public abstract long calculateAccumulatedQuantityOverLevels(int levels);

    public abstract double calculateVwapOverLevels(int levels);

    protected abstract double getTopOfBookPrice();
}
