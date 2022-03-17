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

        this.price = initialQuantity;
        this.quantity = initialQuantity;
    }

    /**
     * Method will adjust the quantity at the current price level
     * @param adjustment to make to price level
     */
    public void adjustQuantity(long adjustment){
        quantity += adjustment;
    }

    public long getPrice() {
        return price;
    }

    public long getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return "PriceLevel{" +
                "price=" + price +
                ", quantity=" + quantity +
                '}';
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
}
