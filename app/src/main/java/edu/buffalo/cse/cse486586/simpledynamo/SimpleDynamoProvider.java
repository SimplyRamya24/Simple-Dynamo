package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.Node;


public class SimpleDynamoProvider extends ContentProvider {

    private SimpleDynamoOpenHelper db;
    static final int SERVER_PORT = 10000;
    private String myNode;
    private ArrayList<NodeObject> nodeList = new ArrayList<>();
    private HashMap<String,String> qallMap ;
    private boolean rejoinHappening = false;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        MessageObject delMsg = new MessageObject();
        delMsg.mType = "D";
        delMsg.mKey = selection;
        delMsg.mMsg = "Delete Message";
        Boolean result;
        // delete in my db or del in others and wait for ack
        for (int i = 0; i < 5 ; i++) {
            int k = (i-1)%5;
            if(k<0)k=4;
            result = checkPartition(delMsg.mKey, nodeList.get(k).mNodeHash,
                    nodeList.get(i).mNodeHash);
            if (result) {
                try {
                    if(nodeList.get(i).mNode.equals(myNode)) {
                        Log.d("Simple Dynamo", "Deleting msg " + delMsg.toString());
                        SQLiteDatabase database = db.getReadableDatabase();
                        String[] sArgs = {selection};
                        database.delete("simple_dynamo", "key = ?", sArgs);
                    } else {
                        vectorMsgs(delMsg, nodeList.get(i).mNode);
                    }
                    vectorMsgs(delMsg, nodeList.get((i+1)%5).mNode);
                    vectorMsgs(delMsg, nodeList.get((i+2)%5).mNode);

                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {

        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        MessageObject insertObj = new MessageObject();

        insertObj.mKey = values.getAsString("key");
        insertObj.mMsg = values.getAsString("value");
        insertObj.mType = "I";
        Log.d("Simple Dynamo","inside insert with "+insertObj.toString());
        boolean result;
        String node;
        for (int i = 0; i < 5 ; i++) {
            int k = (i-1)%5;
            if(k<0)k=4;
            result = checkPartition(insertObj.mKey,nodeList.get(k).mNodeHash,
                    nodeList.get(i).mNodeHash);
            if (result) {
                Log.d("Simple Dynamo","result is true");
                node = nodeList.get((i+1)%5).mNode;
                try {
                    vectorMsgs(insertObj, node);
                    node = nodeList.get((i + 2) % 5).mNode;
                    vectorMsgs(insertObj,node);
                    if(nodeList.get(i).mNode.equals(myNode)) {
                        Log.d("Simple Dynamo","Inserting in self");
                        insertDb(insertObj);
                    } else {
                        Log.d("Simple Dynamo","Sending to owner");
                        node = nodeList.get(i).mNode;
                        vectorMsgs(insertObj,node);
                    }
                } catch (IOException e) {
                    Log.d("Simple Dynamo","IOException in insert");
                    e.printStackTrace();
                }
            }
        }
        return uri;
    }

    private MessageObject vectorMsgs(MessageObject insertObj, String node) throws IOException {

        try {
            Socket socket;
            Log.d("Simple Dynamo", "inside vector msg :"+insertObj.toString());
            MessageObject msgObj = new MessageObject();
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(node));
            socket.setSoTimeout(2400);
            sendMessageToClient(insertObj.toString(), socket);
            BufferedReader inMsg = new BufferedReader
                    (new InputStreamReader(socket.getInputStream()));
            String rMsg = inMsg.readLine();
            //handle failure
            if(rMsg == null) {
               throw new SocketException();
            }
            socket.close();
            String[] rMsgParts = rMsg.split("::");
            Log.d("Received",rMsg);
            if (rMsgParts.length <  3){
                throw new SocketException();
            }
            msgObj.mType = rMsgParts[0];
            msgObj.mKey = rMsgParts[1];
            msgObj.mMsg = rMsgParts[2];
            return msgObj;

        }
        catch(IOException e){
            MessageObject msgObj = new MessageObject();
            msgObj.mMsg = "SocketException";
            msgObj.mType = "Dummy";
            msgObj.mKey = "Exception";
            Log.d("Simple Dynamo","Inside socket exception: "+insertObj.toString());
//            e.printStackTrace();
            return msgObj;
        }
    }

    private void insertDb(MessageObject insertObj) {

        ContentValues value = new ContentValues();
        value.put("key",insertObj.mKey);
        value.put("value",insertObj.mMsg);
        String[] selectionArgs = {value.getAsString("key")};
        Log.d("Simple Dynamo", "Inserting msg " + insertObj.toString());
        SQLiteDatabase database = db.getWritableDatabase();
        Cursor cursor = database.query("simple_dynamo", null, "key = ?",
                selectionArgs, null, null, null);
        if (cursor.getCount() < 1) {
            database.insert("simple_dynamo", null, value);
        }
        else {
            database.update("simple_dynamo", value, "key = ?", selectionArgs);
        }
    }

    @Override
    public boolean onCreate() {
        boolean fileExists = false;
        File fileCheck = getContext().getDatabasePath("simple_dynamo");
        Log.d("Simple Dynamo","file check =" +fileCheck);
        if(fileCheck.exists()) {
            //do a '@'. save in hashmap -> check partition, db
            fileExists = true;

        }
        db = new SimpleDynamoOpenHelper(getContext(), null, null, 1);

        // Logic to get the current instance's node
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService
                (Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myNode = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d("Simple Dynamo", "myNode = "+myNode);
        //Create a server task to listen to incoming msgs
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.d("Simple Dynamo", "Can't create a ServerSocket");
            e.printStackTrace();
            return false;
        }
        // maintain node list
        String tempStr = "11104";
        String hashStr;
        for (int i = 0; i < 5 ; i++) {
            try {
                NodeObject nObj = new NodeObject();
                tempStr = String.valueOf((Integer.parseInt(tempStr) + 4));
                hashStr = String.valueOf(Integer.parseInt(tempStr)/2);
                String value = genHash(hashStr);
                nObj.mNode = tempStr;
                nObj.mNodeHash = value;
                nodeList.add(nObj);
            } catch (NoSuchAlgorithmException e) {
                Log.d("Simple Dynamo", "Can't create a Node object");
                e.printStackTrace();
            }
        }
        Collections.sort(nodeList,new CompareHash());
        for (int i = 0; i < 5; i++) {
            Log.d("Simple Dynamo", "Node "+i+ "=" + nodeList.get(i).mNode
                    +":"+ nodeList.get(i).mNodeHash);
        }

        if (fileExists){
            new JoinTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        SQLiteDatabase database = db.getReadableDatabase();
        switch (selection) {
            case "\"*\"":
                while(rejoinHappening){
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
//                loop to all nodes and combine cursors
                qallMap = new HashMap<>();
                Cursor qCursor = database.rawQuery("SELECT * from simple_dynamo",null);
                while(qCursor.moveToNext()) {
                    String key = qCursor.getString(0);
                    String value = qCursor.getString(1);
                    Log.d("Simple Dynamo", "qallMap key = "+key+" value = "+value);
                    qallMap.put(key,value);
                }
                return queryOtherFour();
            case "\"@\"":
                while(rejoinHappening){
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return database.rawQuery("SELECT * from simple_dynamo",null);
            default:
                while(rejoinHappening){
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                boolean result;
                String queryNode;
                for (int i = 0; i < 5 ; i++) {
                    int k = (i-1)%5;
                    if(k<0)k=4;
                    result = checkPartition(selection,nodeList.get(k).mNodeHash,
                            nodeList.get(i).mNodeHash);
                    if (result) {
                        if(nodeList.get(i).mNode.equals(myNode)) {
                            return queryDB(selection);
                        } else {
                            MessageObject qMsg = new MessageObject();
                            qMsg.mType = "Q";
                            qMsg.mKey = selection;
                            qMsg.mMsg = "Query single msg";
                            queryNode = nodeList.get(i).mNode;
                            Log.d("Simple Dynamo", "Query message = "+qMsg.toString());
                            try {
                                MessageObject queryOneMsgObj = vectorMsgs(qMsg,queryNode);
                                if(queryOneMsgObj.mType.isEmpty() ||
                                        queryOneMsgObj.mMsg.equals("SocketException")) {
                                    //failure logic
                                    Log.d("Simple Dynamo", "node failure -> querying successor");
                                    queryOneMsgObj = vectorMsgs(qMsg,nodeList.get((i+1)%5).mNode);
                                }
                                String[] tablecolumns = {"key","value"};
                                MatrixCursor qOneCursor = new MatrixCursor(tablecolumns);
                                String[] entry = {queryOneMsgObj.mKey,queryOneMsgObj.mMsg };
                                qOneCursor.addRow(entry);
                                return qOneCursor;
                            } catch (IOException e) {
                                Log.d("Simple Dynamo", "IO exception in Q default");
                                e.printStackTrace();
                            }
                        }
                    }
                }
        }
        return null;
    }

    private Cursor queryOtherFour() {
        MessageObject qAllObj = new MessageObject();
        qAllObj.mKey = "\"*\"";
        qAllObj.mType = "QALL";
        qAllObj.mMsg = "Query * msg";
        String qAllnode;
        for (int i = 0; i < 5 ; i++) {
            if(!nodeList.get(i).mNode.equals(myNode)) {
                qAllnode = nodeList.get(i).mNode;
                try {
                    MessageObject qAllmsgObj = vectorMsgs(qAllObj,qAllnode);
                    if(qAllmsgObj.mType.isEmpty() || qAllmsgObj == null ||
                            qAllmsgObj.mMsg.equals("SocketException")) {
                        continue;
                    }
                    if(qAllmsgObj.mMsg.length() < 2) continue;
                    StringBuilder processStr = new StringBuilder(qAllmsgObj.mMsg);
                    processStr.deleteCharAt(qAllmsgObj.mMsg.length()-1);
                    String [] kvRows = processStr.toString().split("&");
                    for(String row:kvRows) {
                        String [] kvpair = row.split("-");
                        Log.d("Simple Dynamo", "qallMap key = " + kvpair[0] + " value = " + kvpair[1]);
                        qallMap.put(kvpair[0],kvpair[1]);
                    }
                } catch (IOException e) {
                    Log.d("Simple Dynamo", "IO exception in Q*");
                    e.printStackTrace();
                }
            }
        }
        String [] columns = {"key","value"};
        MatrixCursor qAllCursor = new MatrixCursor(columns);
        for (String key:qallMap.keySet())
        {
            String value = qallMap.get(key);
            String [] row = {key,value};
            qAllCursor.addRow(row);
        }
        return qAllCursor;
    }

    public Cursor queryDB(String selection) {
        SQLiteDatabase database = db.getReadableDatabase();
        String[] sArgs = {selection};
        Cursor cursor = database.query("simple_dynamo", null, "key = ?", sArgs, null, null, null);
        Log.d("query", selection);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            //logic to receive the messages and pass them to onProgressUpdate()
            Socket clientSocket;
            BufferedReader inMsg;
            try {
                while (true) {
                    clientSocket = serverSocket.accept();
//                    clientSocket.setSoTimeout(2000);
                    inMsg = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String rMsg = inMsg.readLine();
                    if(!(rMsg == null)) {
                        Log.d("Raw", "rMsg ->" + rMsg);
                        //process different message types
                        String[] rMsgParts = rMsg.split("::");
                        MessageObject msgObj = new MessageObject();
                        msgObj.mType = rMsgParts[0];
                        msgObj.mKey = rMsgParts[1];
                        msgObj.mMsg = rMsgParts[2];
                        switch (msgObj.mType) {

                            case "I":
                                insertDb(msgObj);
                                msgObj.mType = "IR";
//                                sendMessageToClient(msgObj.toString(), clientSocket);
                                new SendBgTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
                                        msgObj.toString(),clientSocket);
                                Log.d("Server Task", "Insert signal =" + msgObj.toString());
                                break;
//                        case "IR":
//
//                            break;
                            case "Q":
                                Cursor cursor = queryDB(msgObj.mKey);
                                MessageObject replyMsg = new MessageObject();
                                if (cursor.moveToFirst()) {
                                    replyMsg.mMsg = cursor.getString(1);
                                    replyMsg.mKey = msgObj.mKey;
                                    replyMsg.mType = "QR";
                                    sendMessageToClient(replyMsg.toString(), clientSocket);
                                }
                                Log.d("Server Task", "Query signal =" + replyMsg.toString());
                                break;
//                        case "QR":
//
//                            break;
                            case "QALL":
                                SQLiteDatabase querydb = db.getReadableDatabase();
                                Cursor lcursor = querydb.rawQuery("SELECT * from simple_dynamo", null);
                                MessageObject qAllRObj = new MessageObject();
                                qAllRObj.mType = "QALLR";
                                qAllRObj.mKey = "QAllkey";
                                String msgAll = "";
                                while (lcursor.moveToNext()) {
                                    String key = lcursor.getString(0);
                                    String value = lcursor.getString(1);
                                    msgAll += key + "-" + value + "&";
                                }
                                qAllRObj.mMsg = msgAll;
                                sendMessageToClient(qAllRObj.toString(), clientSocket);
                                Log.d("Server Task", "Query all signal =" + qAllRObj.toString());
                                break;
//                        case "QALLR":
//
//                            break;
                            case "D":
                                SQLiteDatabase database = db.getReadableDatabase();
                                String[] sArgs = {msgObj.mKey};
                                database.delete("simple_dynamo", "key = ?", sArgs);
                                msgObj.mType = "DR";
                                msgObj.mMsg = "DUMMY";
//                                sendMessageToClient(msgObj.toString(), clientSocket);
                                new SendBgTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
                                        msgObj.toString(),clientSocket);
                                Log.d("Server Task", "Delete signal =" + msgObj.toString());
                                break;
//                        case "DR":
//
//                            break;
                        }
                    }
                }
            } catch (IOException e) {
                Log.d("Server Task", "Exception in Server Task");
                e.printStackTrace();
            }
            return null;
        }

    }

    private class JoinTask extends AsyncTask<Void,Void,Void>{

        @Override
        protected Void doInBackground(Void... params) {
            rejoinHappening = true;
            qallMap = new HashMap<>();
            Cursor c = queryOtherFour();
            Log.d("Simple Dynamo","inside jointask");
            for (String key: qallMap.keySet()){
                for (int i = 0; i < 5; i++) {
                    int k = (i-1)%5;
                    k = k<0?4:k;
                    boolean result = checkPartition(key, nodeList.get(k).mNodeHash, nodeList.get(i).mNodeHash);
                    Log.d("Simple Dynamo","jointask Checking "+key);

                    if (result){
                        Log.d("Simple Dynamo","jointask result true");
                        NodeObject successNode =nodeList.get(i);
                        if (successNode.mNode.equals(myNode)||
                                nodeList.get((i+1)%5).mNode.equals(myNode) ||
                                nodeList.get((i+2)%5).mNode.equals(myNode)){
                            Log.d("Simple Dynamo","jointask before insert");
                            MessageObject rejoinObj = new MessageObject();
                            rejoinObj.mKey = key;
                            rejoinObj.mMsg = qallMap.get(key);
                            insertDb(rejoinObj);
                        }
                    }
                }
            }
            rejoinHappening = false;
            return null;
        }
    }

    private class SendBgTask extends AsyncTask<Object, Void, Void>{

        @Override
        protected Void doInBackground(Object... params) {
            String message = (String) params[0];
            Socket socket = (Socket)params[1];
            sendMessageToClient(message,socket);
            return null;
        }
    }

    public boolean checkPartition(String checkString,String node1,String node2 )   {

        try {
            Log.d("CheckPartition","Checking "+checkString);
            Log.d("CheckPartition","Checking node1 = "+node1 +"node2 = " + node2);
            String keyHash = genHash(checkString);
            if(node1.compareTo(node2) > 0) {
                return keyHash.compareTo(node1) > 0 || keyHash.compareTo(node2) <= 0;
            } else if (keyHash.compareTo(node1) > 0 && keyHash.compareTo(node2) <= 0) {
                return true;
            } else {
                return false;
            }
        } catch (NoSuchAlgorithmException e) {
            Log.d("CheckPartition","exception in check partition");
            e.printStackTrace();
            return  true;

        }

    }

    private void sendMessageToClient(String msgToSend, Socket socket) {

        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msgToSend + "\r\n");
        } catch (UnknownHostException e) {
            Log.d("SendMessage", "ClientTask UnknownHostException");
            e.printStackTrace();

        } catch (IOException e) {
            Log.d("SendMessage", "ClientTask socket IOException");
            e.printStackTrace();
        }
    }

}

