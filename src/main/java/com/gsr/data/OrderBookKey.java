package com.gsr.data;

import java.util.Objects;

public class OrderBookKey {

    private final CcyPair pair;
    private final Side side;

    public OrderBookKey(CcyPair pair, Side side) {
        this.pair = pair;
        this.side = side;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderBookKey that = (OrderBookKey) o;
        return pair == that.pair && side == that.side;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pair, side);
    }
}
