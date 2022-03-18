package com.gsr.engine;

import com.gsr.analytics.Request;
import com.gsr.data.*;
import com.gsr.feed.ObjectPool;

import java.util.concurrent.ConcurrentLinkedQueue;

public class OfferOrderBookProcessor extends OrderBookProcessor{

    public OfferOrderBookProcessor(CcyPair pair,
                                   ObjectPool<Message> messageObjectPool,
                                   ConcurrentLinkedQueue<Message> marketDataInboundQueue,
                                   ConcurrentLinkedQueue<Request> analyticsRequestsQueue,
                                   ConcurrentLinkedQueue<Request> analyticsResponseQueue) {

        super(pair,  messageObjectPool, marketDataInboundQueue, analyticsRequestsQueue,analyticsResponseQueue );
    }

    /**
     * Prices are ordered in a sorted double linked list. When a new limit arrives, we traverse the list and insert
     * at the appropriate spot, starting at the top of book as we expect the action to mostly occur there.
     *
     * @param newPriceLevel new price level to be added
     * @param currentPriceLevel price level to compare to, normally start at top of book
     */
    protected void insertPriceInBook(PriceLevel newPriceLevel, PriceLevel currentPriceLevel) {
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
        insertPriceInBook(newPriceLevel, currentPriceLevel.getNextHigher());
    }

    @Override
    protected Side getSide() {
        return Side.Offer;
    }

    @Override
    protected double getTopOfBookPrice() {
        return topOfBook.get() == null ? 0 : (double) topOfBook.get().getPrice() / 100;
    }

    @Override
    public double calculateAveragePrice(int levels) {
        int ptr = 0;
        PriceLevel currLevel = topOfBook.get();

        if(currLevel == null){
            return 0;
        }

        long totalPrice= 0;
        while(ptr <= levels && currLevel != null){
            totalPrice += currLevel.getPrice();
            ptr +=1;
            currLevel = currLevel.getNextHigher();
        }

        //Division by 100 to bring the long representation into double based decimal
        return (double) totalPrice / (ptr * 100);
    }

    @Override
    public long calculateAccumulatedQuantityOverLevels(int levels) {
        int ptr = 0;
        PriceLevel currLevel = topOfBook.get();
        long totalQty= 0;
        while(ptr < levels && currLevel != null){
            totalQty += currLevel.getQuantity();
            ptr +=1;
            currLevel = currLevel.getNextHigher();
        }

        return totalQty;
    }

    @Override
    public double calculateVwapOverLevels(int levels) {
        int ptr = 0;
        PriceLevel currLevel = topOfBook.get();
        double totalPriceWeight= 0;
        while(ptr < levels && currLevel != null){
            totalPriceWeight += (currLevel.getPrice() * currLevel.getQuantity());
            ptr +=1;
            currLevel = currLevel.getNextHigher();
        }

        //Division by 100 to bring the long representation into double based decimal
        return  totalPriceWeight / (calculateAccumulatedQuantityOverLevels(levels) *100);
    }
}
