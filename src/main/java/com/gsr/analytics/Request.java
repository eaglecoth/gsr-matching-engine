package com.gsr.analytics;

import com.gsr.data.CcyPair;
import com.gsr.data.Side;

import java.util.concurrent.atomic.AtomicReference;

public class Request {

    private final int id;
    private final int levels;
    private final RequestType type;
    private final Side side;
    private final CcyPair pair;
    private final AtomicReference<Double> requestResult;

    public Request(int id, int levels, RequestType type, Side side, CcyPair pair) {
        this.id = id;
        this.levels = levels;
        this.type = type;
        this.side = side;
        this.pair = pair;
        this.requestResult = new AtomicReference<>();
    }

    public int getId() {
        return id;
    }

    public int getLevels() {
        return levels;
    }

    public RequestType getType() {
        return type;
    }

    public void populateResult(long value){
        requestResult.set((double) value);
    }

    public void populateResult(double value){
        requestResult.set( value);
    }

    public double getResult(){
        return requestResult.get();
    }

    public Side getSide() {
        return side;
    }

    public CcyPair getPair() {
        return pair;
    }

    @Override
    public String toString() {
        return "Request{" +
                "id=" + id +
                ", levels=" + levels +
                ", type=" + type +
                ", side=" + side +
                ", pair=" + pair +
                ", requestResult=" + requestResult +
                '}';
    }
}
