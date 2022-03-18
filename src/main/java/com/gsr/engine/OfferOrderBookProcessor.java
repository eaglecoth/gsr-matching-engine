package com.gsr.engine;

import com.gsr.analytics.Request;
import com.gsr.data.*;
import com.gsr.feed.ObjectPool;

import java.util.concurrent.ConcurrentLinkedQueue;

public class OfferOrderBookProcessor extends OrderBookProcessor{

    public OfferOrderBookProcessor(CcyPair pair, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Request> requestQueue, ConcurrentLinkedQueue<Request> responseQueue) {
        super(pair,  messageObjectPool, distributorInboundQueue, requestQueue,responseQueue );
    }

    /**
     * Prices are ordered in a sorted double linked list. When a new limit arrives, we traverse the list and insert
     * at the appropriate spot, starting at the top of book as we expect the action to mostly occur there.
     *
     * @param newPriceLevel new price level to be added
     * @param currentPriceLevel price level to compare to, normally start at top of book
     */
    protected void insertInChain(PriceLevel newPriceLevel, PriceLevel currentPriceLevel) {
        if (newPriceLevel.getPrice() < currentPriceLevel.getPrice()) {
            PriceLevel lowerBelowCurrent = currentPriceLevel.getNextLower();
            if (lowerBelowCurrent == null) {
                currentPriceLevel.setNextLower(newPriceLevel);
                newPriceLevel.setNextHigher(currentPriceLevel);
                topOfBook.set(newPriceLevel);
            }else{
                lowerBelowCurrent.setNextHigher(currentPriceLevel);
                newPriceLevel.setNextLower(lowerBelowCurrent);
                newPriceLevel.setNextHigher(currentPriceLevel);
                currentPriceLevel.setNextLower(newPriceLevel);
            }
            return;

        } else if (currentPriceLevel.getNextHigher() == null) {
            currentPriceLevel.setNextHigher(newPriceLevel);
            newPriceLevel.setNextLower(currentPriceLevel);
            return;
        }
        insertInChain(newPriceLevel, currentPriceLevel.getNextHigher());
    }

    @Override
    public void setCorrespondingBook(OrderBookProcessor bidProcessor) {
        this.correspondingProcessor = bidProcessor;
    }


    @Override
    protected boolean priceCrossingSpread(long price) {
        return price <= correspondingProcessor.getTopOfBookPrice();
    }

    @Override
    protected Side getSide() {
        return Side.Offer;
    }

    @Override
    protected long getTopOfBookPrice() {
        return topOfBook == null ? Long.MAX_VALUE : topOfBook.get().getPrice();
    }

    @Override
    public double calculateAveragePrice(int levels) {
        int ptr = 0;
        PriceLevel currLevel = topOfBook.get();
        long totalPrice= 0;
        while(ptr < levels && currLevel != null){
            totalPrice += currLevel.getPrice();
            ptr +=1;
            currLevel = currLevel.getNextHigher();
        }

        return (double) totalPrice / (levels * 100);
    }

    @Override
    public long calculateQtyOverLevels(int levels) {
        return 0;
    }

    @Override
    public double calculateVwapOverLevels(int levels) {
        return 0;
    }
}
