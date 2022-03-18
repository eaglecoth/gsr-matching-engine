package com.gsr.data;


/**
 * Representing an instruction type message from clients to the matching engine for processing
 */
public class Message {

    private volatile MessageType type;
    private volatile CcyPair pair;
    private volatile Side side;
    private volatile long quantity;
    private volatile long price;
    private long time;

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public CcyPair getPair() {
        return pair;
    }

    public void setPair(CcyPair pair) {
        this.pair = pair;
    }

    public long getQuantity() {
        return quantity;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public Side getSide() {
        return side;
    }

    public void setSide(Side side) {
        this.side = side;
    }

    public void populateFields(Message message){
        this.type = message.getType();
        this.pair = message.getPair();
        this.side = message.getSide();
        this.quantity = message.getQuantity();
        this.price = message.getPrice();
    }

    @Override
    public String toString() {
        return "Message{" +
                "type=" + type +
                ", pair=" + pair +
                ", side=" + side +
                ", quantity=" + quantity +
                ", price=" + (double) (price / 100) +
                '}';
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTime() {
        return time;
    }
}
