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
    private final CcyPair pair;

    private final int MAX_PENDING_MD_UPDATES = 100;
    private final int MAX_PENDING_ANALYTICS_REQ = 100;
    private final int MAX_WAIT_NANOS = 2000000000;

    protected volatile boolean runningFlag;
    protected final TreeMap<Long, PriceLevel> orderBook = new TreeMap<>();
    protected final ObjectPool<Message> messageObjectPool;
    private final ObjectPool<PriceLevel> priceLevelObjectPool;
    protected AtomicReference<PriceLevel> topOfBook;

    private final Map<Integer, Long> quantityCalculations = new HashMap<>();
    private final Map<Integer, Double> vwapCalculations = new HashMap<>();
    private final Map<Integer, Double> priceCalculations = new HashMap<>();

    protected volatile OrderBookProcessor correspondingProcessor;

    public OrderBookProcessor(CcyPair pair, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Request> requestQueue, ConcurrentLinkedQueue<Request> responseQueue) {

        this.messageObjectPool = messageObjectPool;
        this.pair = pair;
        this.priceLevelObjectPool = new ObjectPool<>(PriceLevel::new);
        this.topOfBook = new AtomicReference<>(null);

        configureOrderBookThread(distributorInboundQueue, requestQueue, responseQueue);
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
                PriceLevel levelToRemove = orderBook.remove(message.getPrice());
                if (levelToRemove != null && levelToRemove.removePriceFromBook()) {
                    topOfBook.set(null);
                }
                System.out.println("Removed from book: [" + message.getPair() + "] side: [" + message.getSide() + "] price: [" + message.getPrice() +"]");
                messageObjectPool.returnObject(message);
                return;

            case AddOrUpdatePriceLevel:
                    addOrUpdatePriceLevel(message);
                    System.out.println("Added to book: [" + message.getPair() + "] side: [" + message.getSide() + "] price: [" + message.getPrice() +"] Quantity: [" + message.getQuantity() + "]" );
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

        orderBook.compute(message.getPrice(), (priceLevel, limit) -> {

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
     * Helper method to aqcuire a new object form pool and insert in to chain of price levels
     *
     * @param price the price for which no current orders exist
     * @return the newly added price limit
     */
    private PriceLevel addNewPriceLevelToBook(Long price, long quantity) {
        PriceLevel priceLevel = priceLevelObjectPool.acquireObject();
        priceLevel.populate(price, quantity);

        //Unless this is the first price of this book traverse chain and insert.
        //This is not thread safe but it needs not to be as only one thread ever will make modifications
        //on the book.
        if (!topOfBook.compareAndSet(null, priceLevel)) {
            insertInChain(priceLevel, topOfBook.get());
        }
        return priceLevel;
    }

    public void setCorrespondingBook(OrderBookProcessor offerProcessor) {
        this.correspondingProcessor = offerProcessor;
    }

    public void shutdown() {
        System.out.println("Order Book Processor on ccy: [" + pair + "] on side: [" + getSide() + "] shutting down.");
        runningFlag = false;
    }

    /**
     * Helper method to configure the order book thread
     *
     * @param inboundMdQueue      Queue of inbound market data information
     * @param inboundRequestQueue Queue of inbound analytics requests
     * @param outboundQueue       Queue of outbound responses to analytics requests
     */
    private void configureOrderBookThread(final ConcurrentLinkedQueue<Message> inboundMdQueue, final ConcurrentLinkedQueue<Request> inboundRequestQueue, final ConcurrentLinkedQueue<Request> outboundQueue) {
        engineThread = new Thread(() -> {
            System.out.println("Order Book Processor on ccy: [" + pair + "] on side: [" + getSide() + "] started.");

            long lastMdUpdate = Long.MAX_VALUE;
            long lastService = Long.MAX_VALUE;

            while (runningFlag) {

                //How to load balance this ultimately comes down to performance considerations
                //Here I've decided to process all pending requests as long as the other queue does not grow over some
                //tunable limit and the data does not become antiquated. This allows to focus work on the most demanding
                // task.

                //With approach below -- Since analytics results can be cached, we can process huge amounts of them
                //should the market tick slowly. Conversely

                boolean hasUpdatedBook = false;


                while (inboundRequestQueue.size() < MAX_PENDING_ANALYTICS_REQ && hasTimeToExecute(lastService, inboundRequestQueue.isEmpty())) {

                    Message marketDataMessage = inboundMdQueue.poll();

                    if(marketDataMessage != null) {
                        processMessage(marketDataMessage);
                        hasUpdatedBook = true;
                        lastMdUpdate = System.nanoTime();
                    }else{
                        break;
                    }


                }

                if (hasUpdatedBook) {
                    //The cached analytics results are now no longer correct and must be removed
                    clearCalculationResults();
                }

                while (inboundMdQueue.size() < MAX_PENDING_MD_UPDATES && hasTimeToExecute(lastMdUpdate, inboundMdQueue.isEmpty())) {

                    Request request = inboundRequestQueue.poll();

                    if(request != null) {
                        switch (request.getType()) {

                            case Vwap:
                                request.populateResult(vwapCalculations.computeIfAbsent(request.getLevels(), this::calculateVwapOverLevels));
                                break;

                            case AveragePrice:
                                request.populateResult(priceCalculations.computeIfAbsent(request.getLevels(), this::calculateAveragePrice));
                                break;

                            case AverageQuantity:
                                request.populateResult(quantityCalculations.computeIfAbsent(request.getLevels(), this::calculateQtyOverLevels));
                                break;
                        }
                        outboundQueue.add(request);
                        request = inboundRequestQueue.poll();
                        lastService = System.nanoTime();
                    }else{
                        break;
                    }
                }
            }
        }, "OrderBook-" + pair + "-" + getSide());
    }

    private boolean hasTimeToExecute(long lastService, boolean empty) {
        return empty || System.nanoTime() - lastService > MAX_WAIT_NANOS;
    }

    private void clearCalculationResults() {
        priceCalculations.clear();
        vwapCalculations.clear();
        quantityCalculations.clear();
    }

    public void startOrderBook() {
        runningFlag = true;
        engineThread.start();
    }

    //Methods to be implemented depending on side

    abstract void insertInChain(PriceLevel newPriceLevel, PriceLevel currentPriceLevel);

    protected abstract Side getSide();

    public abstract double calculateAveragePrice(int levels);

    public abstract long calculateQtyOverLevels(int levels);

    public abstract double calculateVwapOverLevels(int levels);

    protected abstract double getTopOfBookPrice();
}
