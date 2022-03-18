package com.gsr.engine;

import com.gsr.analytics.Request;
import com.gsr.data.*;
import com.gsr.feed.ObjectPool;

import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Implementation of the Bid side of an order book
 */
public class BidOrderBookProcessor extends OrderBookProcessor{

    public BidOrderBookProcessor(CcyPair pair, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Request> requestQueue, ConcurrentLinkedQueue<Request> responseQueue) {
        super(pair,  messageObjectPool, distributorInboundQueue, requestQueue,responseQueue );
    }

    @Override
    public double calculateAveragePrice(int maxLevel) {

        PriceLevel currentPriceLevel = topOfBook.get();
        long totalPrice= 0;

        if(currentPriceLevel == null){
            //We don't have any price for this side of this pair
            return totalPrice;
        }

        int priceLevelPtr = 0;
        while(priceLevelPtr < maxLevel && currentPriceLevel != null){

            totalPrice += currentPriceLevel.getPrice();
            currentPriceLevel = currentPriceLevel.getNextLower();
            priceLevelPtr +=1;
        }

        //Division by 100 to bring the long representation into double based decimal
        return (double) totalPrice / (priceLevelPtr * 100);
    }

    @Override
    public long calculateAccumulatedQuantityOverLevels(int maxPriceLevel) {

        int priceLevelPtr = 0;
        PriceLevel currentPriceLevel = topOfBook.get();
        long totalQty= 0;
        while(priceLevelPtr < maxPriceLevel && currentPriceLevel != null){
            totalQty += currentPriceLevel.getQuantity();
            priceLevelPtr +=1;
            currentPriceLevel = currentPriceLevel.getNextLower();
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
            currLevel = currLevel.getNextLower();
        }

        //Division by 100 to bring the long representation into double based decimal
        return  totalPriceWeight / (calculateAccumulatedQuantityOverLevels(levels) *100);
    }

    @Override
    protected double getTopOfBookPrice() {
        //Division by 100 to bring the long representation into double based decimal
        return topOfBook.get() == null ? 0 : (double)topOfBook.get().getPrice() / 100;
    }


    /**
     * Prices are ordered in a sorted double linked list. When a new price arrives, we recursively traverse the list
     * and insert at the appropriate spot
     * @param priceToInsert new price level to be added
     * @param currentPriceLevel price level to compare to, normally start at top of book
     */
    protected void insertPriceInBook(PriceLevel priceToInsert, PriceLevel currentPriceLevel) {
        if (priceToInsert.getPrice() > currentPriceLevel.getPrice()) {
            PriceLevel limitAboveCurrent = currentPriceLevel.getNextHigher();
            if (limitAboveCurrent == null) {
                //We're inserting a new best price
                currentPriceLevel.setNextHigher(priceToInsert);
                priceToInsert.setNextLower(currentPriceLevel);
                topOfBook.set(priceToInsert);
            }else{
                //We're inserting a new price somewhere in the middle of the book
                limitAboveCurrent.setNextLower(priceToInsert);
                priceToInsert.setNextHigher(limitAboveCurrent);
                priceToInsert.setNextLower(currentPriceLevel);
                currentPriceLevel.setNextHigher(currentPriceLevel);
            }
            return;

        } else if (currentPriceLevel.getNextLower() == null) {
            //We're inserting a price at the bottom of the book
            currentPriceLevel.setNextLower(priceToInsert);
            priceToInsert.setNextHigher(currentPriceLevel);
            return;
        }
        //Recurse and step to the next limit in the book
        insertPriceInBook(priceToInsert, currentPriceLevel.getNextLower());
    }

    @Override
    protected Side getSide() {
        return Side.Bid;
    }
}
