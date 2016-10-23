package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.CharArrayBuffer;
import android.database.ContentObserver;
import android.database.Cursor;
import android.database.DataSetObserver;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Message;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;
import android.app.Activity;

public class SimpleDhtProvider extends ContentProvider  {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private static Uri mUri;

    private static String myPort ;
    private static String avd ;
    static String myId ;
    //private static String pred ;
    private static String succ ;
    private static String rootNode;
    private static final int SERVER_PORT = 10000;

    private static final ArrayList<String> remote = new ArrayList<String>(Arrays.asList("11108", "11112"  ,"11116", "11120","11124"));

    private static final String reqSplit = "#request#";
    private static final String replySplit = "#reply#";
    private static final String predSplit = "#pred#";
    private static final String ringSplit = "#ringToken#";
    private static final String lQSplit = "#localquery#";
    private static final String gQSplit = "#globalquery#";
    private static final String delLocalSplit = "#deleteLocal#";
    private static final String delGlobalSplit = "#deleteGlobal#";
    private static final String insertCheck = "#insertCheck#";
    private static final String insertFinal = "#insertFinal#";
    private static final String orginKey = "#origin#";
    private static final String selectionKey = "#selection#";

    private static SQLiteDatabase sqLiteDatabase ;
    static final String DATABASE_NAME = "simpleDht" ;
    static final String TABLE_NAME = "messages";
    static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (key TEXT, value TEXT );";
    private static boolean ringComplete = false ;
    private static HashMap<String , String> finalResults;
    private static int count = 0 ;
    private static boolean foundSingleKey = false;


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int count =  sqLiteDatabase.delete(TABLE_NAME, " key=? ", new String[]{selection});
        return count;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = values.getAsString("key");
        String value = values.getAsString("value");


        if(succ.equals(myPort)){
            ContentValues v = new ContentValues();
            v.put("key", key);
            v.put("value", value);
            long rowId  = sqLiteDatabase.insert(TABLE_NAME, null, v);

            //Log.i(TAG, "Inserted - " + key );
        }
        else{
            int c = identifyCaseInsert(key, succ);
            //Log.i(TAG,"Inside Insert - " + key + " " + value + "  case = " + c) ;
            if(c==1||c==2||c==3)
            {
                //insert final
                value = key + insertFinal + value ;
                ///send
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, value, succ);
                //sendReply(value, succ);
            }

            else {
                //forward to succ
                value = key + insertCheck + value ;
                //send
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, value, succ);
                //sendReply(value,succ);
            }
        }


        //long rowId  = sqLiteDatabase.insert(TABLE_NAME, null, values);

        return uri;
    }

    private void insertFinal(String key, String value) {

        ContentValues v = new ContentValues();
        v.put("key", key);
        v.put("value", value);
        long rowId  = sqLiteDatabase.insert(TABLE_NAME, null, v);
        //Log.i(TAG , "Insert Final - inserted  - " + key);

    }

    private void insertCheck(String key, String value) {

        int c = 0;
        c =identifyCaseInsert(key,succ);
        //Log.i(TAG,"Insert Check, case =  " + c);
        if(c==1||c==2||c==3)
        {
            //insert final
            value = key + insertFinal + value ;
            //***
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, value, succ);
            //sendReply(value, succ);
        }

        else {
            //inert in succ - final
            value = key + insertCheck + value ;
            //****
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, value, succ);
            //sendReply(value,succ);
        }

    }


    @Override
    public boolean onCreate() {
        // Log.i(TAG, "Inside On Create!");

        Context context = this.getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf(Integer.parseInt(portStr) * 2);
        Log.i(TAG, "My Port " + myPort);

        SQLiteOpenHelper dbHelper = new DataBaseHelper(getContext());
        sqLiteDatabase = dbHelper.getWritableDatabase();


        rootNode = remote.get(0);
        //queryResults = new HashMap<String, String>();

        try {
            myId = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //pred = myPort;
        succ = myPort;
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        if(!myPort.equals(rootNode)){
            String msg = reqSplit;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
        }


        if(sqLiteDatabase!=null) {
            //sqLiteDatabase.execSQL(CREATE_TABLE);
            //Log.i(TAG, "Database -  " + sqLiteDatabase.isDatabaseIntegrityOk());
            return true;
        }

        else
            return false;
    }

    private class DataBaseHelper extends SQLiteOpenHelper{
        public DataBaseHelper(Context context) {
            super(context, DATABASE_NAME, null,1 );
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            //Log.i(TAG, "Inside on create of private sub class of SQLiteOpenHelper");
            db.execSQL(CREATE_TABLE);
            // Log.i(TAG, "After creating table " + sqLiteDatabase.getAttachedDbs().toString());
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        Log.i(TAG, "Inside Query at " + myPort + " " + selection);

        Cursor cursor;


        if ("@".equals(selection)||"*".equals(selection)) {

            cursor = sqLiteDatabase.rawQuery("select * from " + TABLE_NAME, null);
            Log.i(TAG, "Inside original @ query  of " + myPort + "  " + cursor.getCount());
            return cursor;
        } else {

            String q = "select * from " + TABLE_NAME + " where key = ?";
            Log.i(TAG, "Inside single key query - " + selection);

            cursor = sqLiteDatabase.rawQuery(q, new String[]{selection});
            if (cursor.getCount() > 0)
                return cursor;

            String searchKey = myPort + "#searchKey#" + selection;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, searchKey, succ);

            while (!foundSingleKey) {

            }

            foundSingleKey = false;

            String q_after = "select * from " + TABLE_NAME + " where key = ?";

            Cursor crs = sqLiteDatabase.rawQuery(q_after, new String[]{selection});
            return crs;


        }

    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
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


    private int identifyCase(String reqPort, String succ) {
        int c = 0 ;
        // Log.i(TAG, "Inside identify case - ");
        try {

            String reqId = genHash(String.valueOf(Integer.parseInt(reqPort)/2));
            String succId = genHash(String.valueOf(Integer.parseInt(succ)/2));
            //String predId = genHash(String.valueOf(Integer.parseInt(pred)/2));
            int reqMy = reqId.compareTo(myId);
            int reqSucc = reqId.compareTo(succId);
            int succMy = succId.compareTo(myId);
            //Log.i(TAG, succMy + " ; " + reqMy + " ; " + reqSucc);

            if(succMy>0){
                //succ > my
                if(reqMy>0&&reqSucc<0)
                    c = 1;  //between
            }
            else {
                //succ < my
                if (reqMy > 0 && reqSucc > 0)  // between
                    c = 2;
                if (reqMy < 0 && reqSucc < 0)  // between
                    c = 3;
            }

            return c ;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return c ;
    }
    private int identifyCaseInsert(String key, String succ) {
        int c = 0 ;

        try {

            String reqId = genHash(key);
            String succId = genHash(String.valueOf(Integer.parseInt(succ)/2));
            //String predId = genHash(String.valueOf(Integer.parseInt(pred)/2));
            int reqMy = reqId.compareTo(myId);
            int reqSucc = reqId.compareTo(succId);
            int succMy = succId.compareTo(myId);
            //Log.i(TAG, "succ vs my = " + succMy + " ; " + "key vs my = " +reqMy +  " key vs succ = "  + reqSucc);


            if(succMy>0){
                //succ > my
                if(reqMy>0&&reqSucc<0)
                    c = 1;  //insert final
            }
            else {
                //succ < my
                if (reqMy > 0 && reqSucc > 0)  // before ring end - insert final
                    c = 2;
                if (reqMy < 0 && reqSucc < 0)  // just after ring end - insert final
                    c = 3;
            }
            //Log.i(TAG, "succ vs my = " + succMy + " ; " + "key vs my = " +reqMy +  " key vs succ = "  + reqSucc + " Case =" + c);
            return c ;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return c ;

    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private Socket connection ;
        private ObjectInputStream incoming ;
        private ObjectOutputStream outgoing ;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.i(TAG, "Server Task Thread created ...") ;
            try {
                ServerSocket serverSocket = sockets[0];

                while (true) {

                    connection = serverSocket.accept();
                    incoming = new ObjectInputStream(connection.getInputStream());
                    int o = 0 ;
                    //String cla = incoming.readObject().getClass().toString();
                    Object obj = incoming.readObject();
                    incoming.close();
                    //Log.i(TAG, "Message Receieved - " + obj.toString());
                    //Log.i(TAG, cla);
                    if(obj instanceof String) {

                        String message = (String)obj;
                        //Log.i(TAG, "Message Received -  " + message);
                        if (message.contains(reqSplit)) {

                            String reqPort = message.split(reqSplit)[0];
                            int c = 0;


                            if (myPort.equals(succ)) {
                                //Log.i(TAG, "CASE 0");
                                String reply = myPort + replySplit;
                                succ = reqPort;
                                //sendReply(reply, reqPort);
                                {
                                    Socket socket_new = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(reqPort));
                                    ObjectOutputStream out = new ObjectOutputStream(socket_new.getOutputStream());
                                    out.writeObject(reply);
                                    out.close();
                                    socket_new.close();
                                }

                                //Log.i(TAG, "Sent from " + myPort + " to  " + reqPort + "|| Msg - " + reply);

                            } else{
                                c = identifyCase(reqPort, succ);
                                //Log.i(TAG, "Case Identified - " + c);
                                if (c == 1 || c == 2 || c == 3) {
                                    String reply = succ + replySplit;
                                    succ = reqPort;
                                    //sendReply(reply, reqPort);
                                    {
                                        Socket socket_new = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(reqPort));
                                        ObjectOutputStream out = new ObjectOutputStream(socket_new.getOutputStream());
                                        out.writeObject(reply);
                                        out.close();
                                        socket_new.close();
                                    }
                                    //Log.i(TAG, "Sent from " + myPort + " to  " + reqPort + "|| Msg - " + reply);
                                }

                                else {
                                    String reply = reqPort + reqSplit;
                                    //sendReply(reply, succ);
                                    {
                                        Socket socket_new = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succ));
                                        ObjectOutputStream out = new ObjectOutputStream(socket_new.getOutputStream());
                                        out.writeObject(reply);
                                        out.close();
                                        socket_new.close();
                                    }
                                    //Log.i(TAG, "Sent from " + myPort + " to  " + reqPort + "|| Msg - " + reply);
                                }

                                //Log.i(TAG, " My port - " + myPort + " Succ - " + succ);
                            }
                        }


                        //case reply
                        if (message.contains(replySplit)) {
                            succ = message.split(replySplit)[0];
                            //Log.i(TAG, " My port - " + myPort + " Succ - " + succ);
                        }


                        if (message.contains("#searchKey#")) {

                            String org  = message.split("#searchKey#")[0];
                            String key = message.split("#searchKey#")[1];
                            Log.i(TAG,"Inside search key - " + " From - " + org+ " key - " + key);

                            String q = "select * from " + TABLE_NAME + " where key = ? " ;
                            Cursor cursor = sqLiteDatabase.rawQuery( q  , new String[]{key});
                            Log.i(TAG,"Found on local ? " + cursor.getCount());
                            if(cursor.getCount()>0){
                                {
                                    cursor.moveToFirst();
                                    String k = cursor.getString(cursor.getColumnIndex("key"));
                                    String value = cursor.getString(cursor.getColumnIndex("value"));
                                    cursor.close();
                                    String found = k+"#found#"+value;
                                    Log.i(TAG , " Found - " + found);

                                    Socket socket_new = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(org));
                                    ObjectOutputStream out = new ObjectOutputStream(socket_new.getOutputStream());
                                    out.writeObject(found);
                                    out.close();
                                    socket_new.close();
                                }

                            }
                            else{
                                Log.i(TAG, "Not found , passing to " + succ);
                                Socket socket_new = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succ));
                                ObjectOutputStream out = new ObjectOutputStream(socket_new.getOutputStream());
                                out.writeObject(message);
                                out.close();
                                socket_new.close();
                            }
                        }

                        if (message.contains("#found#")) {
                            String k = message.split("#found#")[0];
                            String v = message.split("#found#")[1];
                            Log.i(TAG, "Found at " + myPort);

                            ContentValues cv = new ContentValues();
                            cv.put("key", k);
                            cv.put("value", v);
                            long rowId  = sqLiteDatabase.insert(TABLE_NAME, null, cv);

                            foundSingleKey = true ;
                        }


                        if (message.contains(insertCheck)) {
                            String key = message.split(insertCheck)[0];
                            String value = message.split(insertCheck)[1];
                            insertCheck(key, value);
                        }

                        if (message.contains(insertFinal)) {
                            String key = message.split(insertFinal)[0];
                            String value = message.split(insertFinal)[1];
                            insertFinal(key, value);
                        }

                    }


                    //Log.i(TAG, " My port - " + myPort + " Succ - " + succ);

                }

            }
            catch (IOException e) {
                e.printStackTrace();
            }

            catch (NullPointerException e) {
                e.printStackTrace();
            }

            catch (Exception e) {
                e.printStackTrace();
            }

            // Log.i(TAG , "Message backStrings " + backStrings ) ;

            return null;

        }



        protected void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();
            Log.i(TAG , "Inside onProgess Update, Message String Received  - " + strReceived);


            try {
             /* resolver = getContentResolver();
                ContentValues values = new ContentValues();
                values.put("key", Integer.toString(id));
                values.put("value", strReceived);
                resolver.insert(mUri, values);
                id++;*/
            } catch (Exception e){
                e.printStackTrace();
            }


            return;
        }
    }



    private class ClientTask extends AsyncTask<Object, Void, Void> {


        @Override
        protected Void doInBackground(Object... msgs) {
            Socket socket;
            Object messageType = msgs[0];
            String sendTo = (String)msgs[1];

            try {

                if(messageType instanceof HashMap){
                    HashMap hashMap = (HashMap)messageType;
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendTo));
                    ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
                    outgoing.writeObject(hashMap);
                    outgoing.close();
                    socket.close();

                }

                else if(messageType instanceof String){
                    String messageToSend = (String)messageType;
                    if(messageToSend.contains(reqSplit)){
                        String finalMsg = myPort + reqSplit;
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote.get(0)));
                        ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
                        outgoing.writeObject(finalMsg);
                        outgoing.close();
                        socket.close();

                    }

                    else{
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendTo));
                        ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
                        outgoing.writeObject(messageToSend);
                        outgoing.close();
                        socket.close();
                    }
                }


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }
            catch (NullPointerException e) {
                Log.e(TAG, "ClientTask socket NUllPointerException");
                e.printStackTrace();
            }
            catch (Exception e) {
                Log.e(TAG, "ClientTask socket Exception");
                e.printStackTrace();
            }

            return null;
        }
    }


}