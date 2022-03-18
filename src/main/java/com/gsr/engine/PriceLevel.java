package com.gsr.engine;

/**
 * Representation of one price level for a particular side of an order book.  The limit level holds a double
 * linked list of orders in order to allow O(1) matching of orders and cancelation of orders.
 */
public class PriceLevel {

    private long price;
    private long quantity;
    private PriceLevel nextHigher;
    private PriceLevel nextLower;

    public PriceLevel(){}

    public void populate(Long price, long initialQuantity) {

        this.price = price;
        this.quantity = initialQuantity;
    }

    /**
     * Method will adjust the quantity at the current price level
     * @param adjustment to make to price level
     */
    public void adjustQuantity(long adjustment){
        quantity = adjustment;
    }

    public long getPrice() {
        return price;
    }

    public long getQuantity() {
        return quantity;
    }

    public PriceLevel getNextHigher() {
        return nextHigher;
    }

    public PriceLevel getNextLower() {
        return nextLower;
    }

    public void setNextHigher(PriceLevel nextHigher) {
        this.nextHigher = nextHigher;
    }

    public void setNextLower(PriceLevel nextLower) {
        this.nextLower = nextLower;
    }

    /**
     * If the quantity of the price is 0, the level does no longer serve any purpose and should be removed
     * from book
     * @return true if this limit was the last in the book
     */
    public boolean removePriceFromBook(){
        if(nextHigher != null && nextLower != null){
            nextHigher.setNextLower(nextLower);
            nextLower.setNextHigher(nextHigher);
            return false;
        }else if(nextHigher != null){
            nextHigher.setNextLower(null);
            return false;
        }else if(nextLower != null){
            nextLower.setNextHigher(null);
            return false;
        }else{
            return true;
        }
    }

    @Override
    public String toString() {
        return "PriceLevel{" +
                "price=" + price +
                ", quantity=" + quantity +
                '}';
    }

}
