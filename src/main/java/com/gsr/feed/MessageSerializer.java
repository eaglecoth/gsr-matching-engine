package com.gsr.feed;

public interface MessageSerializer {

    boolean onMessage(String message);
}
