package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    ContentResolver resolver ;
    private static Uri mUri;
    static int id = 0 ;
    static final ArrayList<String> remote = new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120","11124"));

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));*/

        // mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger1.provider");
        uriBuilder.scheme("content");
        mUri = uriBuilder.build();


        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }



        final EditText editText = (EditText) findViewById(R.id.editText1);

       /* resolver = getContentResolver();
        String value = editText.getText().toString();
        ContentValues values = new ContentValues();
        values.put(myPort, value);
        resolver.insert(Uri.parse(URL), values);*/




        findViewById(R.id.button4).setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        String msg = editText.getText().toString();

                        //TextView localTextView = (TextView) findViewById(R.id.textView1);

                        Log.i(TAG, "Msg to print - " + msg);
                        //localTextView.append("\t" + msg);
                        //TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
                        //remoteTextView.append("\n");
                        editText.setText("");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

                    }
                }
        );

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private Socket connection ;
        private ObjectInputStream incoming ;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.i(TAG, "Server Task Thread created ...") ;
            try {
                ServerSocket serverSocket = sockets[0];
                //connection = serverSocket.accept();
                try {
                    while (true){

                        connection = serverSocket.accept();
                        if(connection != null )
                            Log.i(TAG, "Connection request from remote client accepted!");
                        incoming = new ObjectInputStream(connection.getInputStream());
                        if(incoming!=null)
                            Log.i(TAG, "Input stream setup!");
                        if(incoming==null || connection.isInputShutdown()) {
                            Log.e(TAG, "Problem in connection. ");
                            break;
                        }
                        String message = (String) incoming.readObject();
                        Log.i(TAG, "Message received at server - " + message.trim() + " Socket address - " + connection.getLocalSocketAddress());
                        if(message!=null)
                            // onProgressUpdate(message.trim());
                            publishProgress(message.trim());

                    }
                }
                catch (ClassNotFoundException e){
                    e.printStackTrace();

                }

            } catch (IOException e) {
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
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            Log.i(TAG , "Inside onProgess Update, Message String Received  - " + strReceived);
            //TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
            //remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append(strReceived + "\t\n");


            try {
                resolver = getContentResolver();
                ContentValues values = new ContentValues();
                //String id_string = Integer.toString(id);
                values.put("key", Integer.toString(id));
                //resolver.insert(Uri.parse(URL), values);
                values.put("value", strReceived);
                Log.i(TAG, "Values -  " + values.valueSet().toString() + " mURI - " + mUri.toString());
                resolver.insert(mUri, values);
                id++;
            } catch (Exception e){
                e.printStackTrace();
            }


            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {

            try {


               // String myPort = msgs[1].toString();
                ObjectOutputStream outgoing = null ;
                Socket socket ;


                //Log.i(TAG, "My port  - " + myPort);
                String msgToSend = msgs[0];

                for(String s : remote ) {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s));
                    Log.i(TAG, "Socket created at  " + socket.getPort() + "  " + socket.getLocalPort() + " Sending message - " + msgToSend);
                    if(socket!=null)
                        outgoing = new ObjectOutputStream(socket.getOutputStream());
                    if(outgoing!=null)
                        outgoing.writeObject(msgToSend);
                    outgoing.flush();
                    socket.close();
                    Log.i(TAG, "Socket at " + socket.getPort() +  " closed. ");

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



