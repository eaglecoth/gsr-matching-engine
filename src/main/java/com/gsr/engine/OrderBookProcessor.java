package com.gsr.engine;

import com.gsr.data.*;
import com.gsr.feed.ObjectPool;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The OrderBookProcessor is an instance to represent and manage one side of a book for a particular currency pair
 * It holds price levels in a pseudo linked list
 */
public abstract class OrderBookProcessor {
    private Thread engineThread;
    private final CcyPair pair;
    protected ConcurrentLinkedQueue<Message> distributorInboundQueue;
    protected volatile boolean runningFlag;
    protected final TreeMap<Long, PriceLevel> orderBook = new TreeMap<>();
    protected final ObjectPool<Message> messageObjectPool;
    private final ObjectPool<PriceLevel> limitObjectPool;
    protected volatile PriceLevel topOfBook;


    protected volatile OrderBookProcessor correspondingProcessor;

    public OrderBookProcessor(CcyPair pair, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue) {
        this.distributorInboundQueue = distributorInboundQueue;
        this.messageObjectPool = messageObjectPool;
        this.pair = pair;
        this.limitObjectPool = new ObjectPool<>(PriceLevel::new);

        configureOrderBookThread(distributorInboundQueue);
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
                orderBook.remove(message.getPrice());
                messageObjectPool.returnObject(message);
                return;

            case AddOrUpdatePriceLevel:

                if(!priceCrossingSpread(message.getPrice())){
                    addOrUpdatePriceLevel(message);
                }
                messageObjectPool.returnObject(message);
        }

    }

    /**
     * Helper method to add or update quantity at a price, and if such price does not exist
     * then create it
     * @param message containing a quantity which is to be inserted into the book.
     */
    private void addOrUpdatePriceLevel(final Message message) {

        orderBook.compute(message.getPrice(), (priceLevel, limit) -> {

            //If this is the first order of this price create the new limit book
            if(limit == null) {
                limit = addNewPriceLevelToBook(priceLevel, message.getQuantity());
            }else{
                limit.adjustQuantity(message.getQuantity());
            }
            return limit;
        });
    }

    /**
     * Helper method to aqcuire a new object form pool and insert in to chain of price levels
     * @param priceLevel the price for which no current orders exist
     * @return  the newly added price limit
     */
    private PriceLevel addNewPriceLevelToBook(Long priceLevel, long quantity) {
        PriceLevel limit = limitObjectPool.acquireObject();
        limit.populate(priceLevel, quantity);
        //Unless this is the first order entirely in this book, traverse the list and insert the new limit
        //where it fits in.
        if (topOfBook != null) {
            insertInChain(limit, topOfBook);
        } else {
            topOfBook = limit;
        }
        return limit;
    }

    public ConcurrentLinkedQueue<Message> getDistributorInboundQueue() {
        return distributorInboundQueue;
    }

    public CcyPair getPair() {
        return pair;
    }

    public void setCorrespondingBook(OrderBookProcessor offerProcessor) {
        this.correspondingProcessor = offerProcessor;
    }

    public void shutdown() {
        System.out.println("Order Book Processor on ccy: [" + pair + "] on side: [" + getSide() +"] shutting down.");
        runningFlag = false;
    }

    /**
     * Helper method to launch the processor in its own thread.
     * @param distributorInboundQueue Queue for which to poll for incoming orders / cancellations
     */
    private void configureOrderBookThread(ConcurrentLinkedQueue<Message> distributorInboundQueue) {
        engineThread = new Thread(() -> {
            System.out.println("Order Book Processor on ccy: [" + pair + "] on side: [" + getSide() +"] started.");

            while (runningFlag) {
                Message message = distributorInboundQueue.poll();
                if (message != null) {
                    processMessage(message);
                }
            }
        });
    }

    public void startOrderBook(){
        runningFlag = true;
        engineThread.start();
    }

    //Methods to be implemented depending on side

    abstract void insertInChain(PriceLevel newPriceLevel, PriceLevel currentPriceLevel);

    abstract PriceLevel getNextLevelLimit(PriceLevel priceLevelToExecute);

    protected abstract Side getSide();

    protected abstract Side getOppositeSide();

    public abstract double calculateAveragePrice(int levels);

    public abstract long calculateQtyOverLevels(int levels);

    public abstract double calculateVwapOverLevels(int levels);

    protected abstract long getTopOfBookPrice();

    protected abstract boolean priceCrossingSpread(long price);

}
