package com.gsr.feed;

import com.gsr.data.CcyPair;
import com.gsr.data.Message;
import com.gsr.data.MessageType;
import com.gsr.data.Side;

import java.util.Queue;


/**
 * Not sure if this was really necessary.  The engine needs some serializer mechanism. This one is stupid.
 */
public class MessageSerializerImpl implements MessageSerializer {

    private final Queue<Message> engineMessageQueue;
    private final ObjectPool<Message> messageObjectPool;
    private final String stringDelimiter;
    private final String valueDelimiter;
    private final long offerRetryCount;
    private final long sleepTimeMillis;

    public MessageSerializerImpl(Queue<Message> messageQueue, ObjectPool<Message> objectPool, long retryCount, long waitTimeMillis, String delimiter, String keyValueDelimiter) {

        engineMessageQueue = messageQueue;
        messageObjectPool = objectPool;
        offerRetryCount = retryCount;
        sleepTimeMillis = waitTimeMillis;
        stringDelimiter = delimiter;
        valueDelimiter = keyValueDelimiter;
    }

    /**
     * @param messageString instruction to send to matching engine
     * @return true if message was successfully submitted, else false
     */
    public boolean onMessage(String messageString) {

        Message message = deserialize(messageString);
        if (message == null) {
            return false;
        }
        System.out.println("Parsed message: " + message);

        if (!engineMessageQueue.offer(message)) {
            long currentRetryCount = offerRetryCount;

            while (!engineMessageQueue.offer(message) && currentRetryCount > 0) {
                System.out.println("ERROR: Queue is full.  What do I do now? Just wait?");
                try {
                    Thread.sleep(sleepTimeMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return false;
                }
                currentRetryCount -= 1;
            }
        }

        return true;
    }


    private Message deserialize(String msgToDeSerialize) {

        if(msgToDeSerialize.charAt(0) == '#'){
            System.out.println("Comment ignored: ");
            return null;
        }


        String[] messageString = msgToDeSerialize.split(stringDelimiter);
        Message message = messageObjectPool.acquireObject();

        int ptr = 0;

        while (ptr < messageString.length) {

            switch (messageString[ptr].charAt(0)) {
                case 't':
                    message.setTime(Long.parseLong(messageString[ptr].split(valueDelimiter)[1]));
                    break;
                case 'i':
                    message.setPair(CcyPair.valueOf(messageString[ptr].split(valueDelimiter)[1]));
                    break;
                case 'p':
                    message.setPrice(parsePrice(messageString[ptr].split(valueDelimiter)[1]));
                    break;
                case 'q':
                    String quantity = messageString[ptr].split(valueDelimiter)[1];
                    if ("0".equals(quantity) || "0.0".equals(quantity) || "0.00".equals(quantity)) {
                        message.setType(MessageType.RemovePriceLevel);
                    } else {
                        message.setType(MessageType.AddOrUpdatePriceLevel);
                        message.setQuantity(Long.parseLong(messageString[ptr].split(valueDelimiter)[1]));
                    }
                    break;
                case 's':
                    message.setSide(messageString[ptr].split(valueDelimiter)[1].equals("b") ? Side.Bid : Side.Offer);
                    break;
                default:
                    System.out.println("I don't understand this field " + messageString[ptr].charAt(0) + ". I will ignore it");
            }
            ptr += 1;
        }

        return message;
    }

    /**
     * We do all representation with 2 decimals.
     * @param priceString string representation of price to parse
     * @return long representing  the price * 100. I.e. last 2 digits are the decimal points -- padded if required.
     */
    private long parsePrice(String priceString) {
        String[] split = priceString.split("\\.");

        if(split.length == 1){
            return Long.parseLong(priceString) * 100;
        }else{
            long result = Long.parseLong(split[0] + split[1]);
            if(split[1].length() == 1){
                result  = result * 10;
            }
            return result;
        }
    }
}
