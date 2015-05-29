package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by ramya on 5/2/15.
 */
public class MessageObject {

    public String mType;
    public String mMsg;
    public String mKey;

    public MessageObject() {

    }
    @Override
    public String toString() {
        return mType + "::" +
               mKey +"::"+
               mMsg;
    }
}
