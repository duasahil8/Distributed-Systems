package edu.buffalo.cse.cse486586.groupmessenger2;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

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
    static final ArrayList<String> remote = new ArrayList<String>(Arrays.asList("11108", "11112"
            ,"11116", "11120"
            ,"11124"
    ));
    private final String splitOrg = "#org#";
    private final String splitSeq = "#seq#";
    private final String splitFinal = "#final#";
    private static String myPort ;
    private static int index ;
    private static int counter =0;
    private static int sequence = 0;
    private static int alive;
    private static ArrayList<QMessage> queue = new ArrayList<QMessage>();
    private static ArrayList<SeqMessage> seqList = new ArrayList<SeqMessage>();
    private static ArrayList<String> crash = new ArrayList<String>();
    @Override
    protected void onCreate(Bundle savedInstanceState) {


        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        index = ((Integer.parseInt(portStr)*2)-11108)/4;
        Log.i(TAG, "My port - " + myPort);


        // mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
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

                        //Log.i(TAG, "Msg to print - " + msg);
                        //localTextView.append("\t" + msg);
                        //TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
                        //remoteTextView.append("\n");
                        editText.setText("");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

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

    public class QComparator implements Comparator<QMessage>{
        @Override
        public int compare(QMessage lhs, QMessage rhs) {

            if(Double.valueOf(lhs.getSequence()) < Double.valueOf(rhs.getSequence()))return -1;
            if(Double.valueOf(lhs.getSequence()) > Double.valueOf(rhs.getSequence()))return 1;
            if(Double.valueOf(lhs.getSequence()) == Double.valueOf(rhs.getSequence())){

                if(!lhs.isDeliverable()&& rhs.isDeliverable()) return -1;
                if(Double.valueOf(lhs.getSequence()) > Double.valueOf(rhs.getSequence()))return 1;
                if(lhs.isDeliverable()== rhs.isDeliverable()){
                    if(Integer.valueOf(lhs.getRecvAvd())<Integer.valueOf(rhs.getRecvAvd())) return -1;
                    else return 1;
                }
            }
            return 0;


           /* if(Integer.valueOf(lhs.getSequence()) < Integer.valueOf(rhs.getSequence()))return -1;
            if(Integer.valueOf(lhs.getSequence()) > Integer.valueOf(rhs.getSequence()))return 1;
            if(Integer.valueOf(lhs.getSequence()) == Integer.valueOf(rhs.getSequence())){

                if(!lhs.isDeliverable()&& rhs.isDeliverable()) return -1;
                if(Integer.valueOf(lhs.getSequence()) > Integer.valueOf(rhs.getSequence()))return 1;
                if(lhs.isDeliverable()== rhs.isDeliverable()){
                    if(Integer.valueOf(lhs.getRecvAvd())<Integer.valueOf(rhs.getRecvAvd())) return -1;
                    else return 1;
                }
            }
            return 0;*/

        }
    }

    private class SeqMessage{

        private String messageId ;
        private String sequence ;
        private String fromAvd;

        @Override
        public String toString() {
            return "SeqMessage{" +
                    "messageId='" + messageId + '\'' +
                    ", sequence='" + sequence + '\'' +
                    ", fromAvd='" + fromAvd + '\'' +
                    '}';
        }

        public SeqMessage(String messageId, String sequence, String fromAvd) {
            this.messageId = messageId;
            this.sequence = sequence;
            this.fromAvd = fromAvd;
        }

        public String getMessageId() {
            return messageId;
        }

        public void setMessageId(String messageId) {
            this.messageId = messageId;
        }

        public String getSequence() {
            return sequence;
        }

        public void setSequence(String sequence) {
            this.sequence = sequence;
        }

        public String getFromAvd() {
            return fromAvd;
        }

        public void setFromAvd(String fromAvd) {
            this.fromAvd = fromAvd;
        }
    }

    private class QMessage{
        private String message ;
        private String messageId ;
        private String fromAvd ;
        private String sequence ;
        private String recvAvd ;
        private boolean deliverable ;

        @Override
        public String toString() {
            return
                    "[" + message + ", " + messageId  + ", " + fromAvd + ", " + sequence + ", " + recvAvd + ", " + deliverable + "] " ;

        }



        public QMessage(String message, String messageId, String fromAvd, String sequence, String recvAvd, boolean deliverable) {
            this.message = message;
            this.messageId = messageId;
            this.fromAvd = fromAvd;
            this.sequence = sequence;
            this.recvAvd = recvAvd;
            this.deliverable = deliverable;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getMessageId() {
            return messageId;
        }

        public void setMessageId(String messageId) {
            this.messageId = messageId;
        }

        public boolean isDeliverable() {
            return deliverable;
        }

        public void setDeliverable(boolean deliverable) {
            this.deliverable = deliverable;
        }

        public String getFromAvd() {
            return fromAvd;
        }

        public void setFromAvd(String fromAvd) {
            this.fromAvd = fromAvd;
        }

        public String getSequence() {
            return sequence;
        }

        public void setSequence(String sequence) {
            this.sequence = sequence;
        }

        public String getRecvAvd() {
            return recvAvd;
        }

        public void setRecvAvd(String recvAvd) {
            this.recvAvd = recvAvd;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private Socket connection ;
        private ObjectInputStream incoming ;
        private ObjectOutputStream outgoing ;



        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            //Log.i(TAG, "Server Task Thread created ...") ;
            try {
                ServerSocket serverSocket = sockets[0];
                //connection = serverSocket.accept();
                try {
                    while (true){

                        if(!queue.isEmpty()) {
                            Collections.sort(queue, new QComparator());
                            Log.i(TAG, "Sorted Q - " + "Size - " + queue.size() + " ||" + queue.toString());
                            QMessage head = queue.get(0);
                            if(head.isDeliverable()) {
                                Log.i(TAG, "Head is deliverable! ");
                                //queue.remove(head);
                                publishProgress(head.getMessage());
                                //Log.i(TAG, "After publish progress Q size " + queue.size());
                                queue.remove(head);
                            }
                        }

                        if(!queue.isEmpty()) {
                            boolean allTrue = true;
                            for(QMessage qm : queue){
                                if(!qm.isDeliverable()) allTrue = false;
                            }
                            if(allTrue) {
                                deliverAll();
                            }
                        }


                        connection = serverSocket.accept();
                        if(connection!=null) {
//
                            incoming = new ObjectInputStream(connection.getInputStream());

//                        if(incoming!=null)
                            // Log.i(TAG, "Input stream setup!");
//                        if(incoming==null || connection.isInputShutdown()
//                               // ||!connection.getKeepAlive()
//                                ) {
//                            //alive-- ;
//                            //Log.e(TAG, "Problem in connection. Alive Avds - " + alive);
//                            //break;
//                        }
                            String message = (String) incoming.readObject();
                            Log.i(TAG, "Message received at " + myPort + "  " + message.trim());
                            if (message != null) {
                                //case1
                                if (message.contains(splitOrg)) {
                                    sequence = sequence + 1;
                                    String msgSplit[] = message.split(splitOrg);
                                    //OriginalMessage msg = new OriginalMessage(msgSplit[0],msgSplit[1],msgSplit[2]);
                                    //Log.i(TAG,"Inside Original Message " + message);


                                    String s = String.valueOf(sequence)
                                            + "." + String.valueOf(index);

                                    SeqMessage seqMessage = new SeqMessage(msgSplit[1], s, myPort);
                                    String seqMessageString = msgSplit[1] + splitSeq + s + splitSeq + myPort;

                                    //seqList.add(seqMessage);

                                    QMessage qMessage = new QMessage(msgSplit[0], msgSplit[1], msgSplit[2], s, myPort, false);
                                    queue.add(qMessage);
                                    //changeQueue();

                                    if(!queue.isEmpty()) {
                                        Collections.sort(queue, new QComparator());
                                        Log.i(TAG, "Sorted Q - " + "Size - " + queue.size() + " ||" + queue.toString());
                                        QMessage head = queue.get(0);
                                        if(head.isDeliverable()) {
                                            Log.i(TAG, "Head is deliverable! ");
                                            //queue.remove(head);
                                            publishProgress(head.getMessage());
                                            //Log.i(TAG, "After publish progress Q size " + queue.size());
                                            queue.remove(head);
                                        }
                                    }


                                    Socket sockNew = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgSplit[2]));
                                    //sockNew.setKeepAlive(true);
                                    ObjectOutputStream outNew = new ObjectOutputStream(sockNew.getOutputStream());

                                    outNew.writeObject(seqMessageString);
                                    outNew.flush();
                                    outNew.close();
                                    //Log.i(TAG, "Sent  - " + msgSplit[2] + " " + seqMessageString);
                                /*outgoing = new ObjectOutputStream(connection.getOutputStream());
                                outgoing.writeObject(seqMessageString);
                                outgoing.flush();
                                outgoing.close();*/

                                }
                                //case 2
                                if (message.contains(splitSeq)) {
                                    int count = 0;
                                    double max = 0.0;
                                    //int max = 0;
                                    String msgSplit[] = message.split(splitSeq);
                                    SeqMessage seqMessage = new SeqMessage(msgSplit[0], msgSplit[1], msgSplit[2]);
                                    //Log.i(TAG,"Inside Sequence Message  " + message);
                                    seqList.add(seqMessage);
                                    //Log.i(TAG, "Sequnce Message List : " + seqList.size() + "  " + seqList);

                                    max = Double.valueOf(msgSplit[1]);
                                    int m = (int) max;
                                    //max = Integer.valueOf(msgSplit[1]);
                                    SeqMessage highestSeq = seqMessage;
                                    //Log.i(TAG, "Sequence Message Size - " + seqList.size());
                                    //Log.i(TAG, "Count - " + count);
                                    for (SeqMessage seqm : seqList) {
                                        //Log.i(TAG,"Msg Id Match ? " + seqMessage.getMessageId().equals(seqm.getMessageId()));

                                        if (seqm.getMessageId().equals(seqMessage.getMessageId())) {
                                            count = count + 1;
                                            //Log.i(TAG, "Count after msg Id match " + count);
                                            Double curr = Double.valueOf(seqm.getSequence());
                                            int c = curr.intValue();

                                            if (curr == max) {
                                                //Double.valueOf(seqm.getSequence())== max){
                                                if (Integer.valueOf(seqm.getFromAvd()) < Integer.valueOf(highestSeq.getFromAvd())) {
                                                    max = Double.valueOf(seqm.getSequence());
                                                    //max = Integer.valueOf(seqm.getSequence());
                                                    highestSeq = seqm;
                                                }

                                            }

                                            if (curr > max
                                                //Integer.valueOf(seqm.getSequence())> max
                                                    ) {
                                                max = Double.valueOf(seqm.getSequence());
                                                //max = Integer.valueOf(seqm.getSequence());
                                                highestSeq = seqm;
                                            }

                                        }

                                    }
                                    //Log.i(TAG, "Count of sequence msg for avd  " + index + " Message - " + seqMessage.getMessageId() + " : " + count);
                                    alive = remote.size() - crash.size();
                                    if (count == alive) {
                                        Socket sock;
                                        //FinalMessage finalMessage = new FinalMessage(seqMessage.getMessageId(),myPort,String.valueOf(max),highestSeq.getFromAvd());
                                        String finalMessageString = seqMessage.getMessageId() + splitFinal + myPort + splitFinal + String.valueOf(max) + splitFinal + highestSeq.getFromAvd();

                                        for (String s : remote) {
                                            sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s));

                                            if (sock != null) {
                                                ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
                                                out.writeObject(finalMessageString);
                                                out.flush();
                                                out.close();
                                                //Log.i(TAG, "Sent broadcast final to " + s);
                                            }
                                            sock.close();

                                        }

                                    }


                                }
                                //case 3
                                if (message.contains(splitFinal)) {

                                    String msgSplit[] = message.split(splitFinal);

                                    double highestProposed = Double.valueOf(msgSplit[2]);
                                    //Integer highestProposed = Integer.valueOf(msgSplit[2]);
                                    //update own sequence

                                /*if(sequence<=(int)highestProposed)
                                    sequence = (int)highestProposed;*/

                                    //***check this

                                    if (sequence <= highestProposed) {
                                        sequence = (int) highestProposed;
                                    }

                                    QMessage qNew = null;
                                    QMessage qRemove = null;

                                    for (QMessage q : queue) {
                                        if (q.getMessageId().equals(msgSplit[0])
                                                && q.getFromAvd().equals(msgSplit[1])) {
                                            //Log.i(TAG, "Inside final Q iteration - " + q.getMessageId() + " " + q.getRecvAvd());
                                            qNew = q;
                                            qRemove = q;

                                            qNew.setSequence(String.valueOf(highestProposed));
                                            qNew.setRecvAvd(msgSplit[3]);
                                            qNew.setDeliverable(true);

                                        }
                                    }
                                    if (qRemove != null) {
                                        queue.remove(qRemove);
                                        queue.add(qNew);
                                        if(!queue.isEmpty()) {
                                            Collections.sort(queue, new QComparator());
                                            Log.i(TAG, "Sorted Q - " + "Size - " + queue.size() + " ||" + queue.toString());
                                            QMessage head = queue.get(0);
                                            if(head.isDeliverable()) {
                                                Log.i(TAG, "Head is deliverable! ");
                                                //queue.remove(head);
                                                publishProgress(head.getMessage());
                                                //Log.i(TAG, "After publish progress Q size " + queue.size());
                                                queue.remove(head);
                                            }
                                        }

                                    }


                                    // onProgressUpdate(message.trim());
                                    //publishProgress(message.trim());
                                }
                            }
                        }
                        /*else{
                            while(!queue.isEmpty()){
                                changeQueue();
                                Log.i(TAG,"Log 2");
                            }

                        }*/

                       /* if(!queue.isEmpty()){
                            changeQueue();
                            Log.i(TAG, "Log 3");

                        }*/


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


           /* while(!queue.isEmpty()){
                changeQueue();
                Log.i(TAG, "Log 4");

            }*/

            return null;

        }
        protected void deliverAll(){
            Collections.sort(queue, new QComparator()) ;

            for(QMessage qm : queue) {
                    publishProgress(qm.getMessage());
            }
            queue.clear();
        }

        /*protected void changeQueue(){
            if(!queue.isEmpty()) {
                Collections.sort(queue, new QComparator());
                Log.i(TAG, "Sorted Q - " + "Size - " + queue.size() + " ||" + queue.toString());
                QMessage head = queue.get(0);
                if(head.isDeliverable()) {
                    Log.i(TAG, "Head is deliverable! ");
                    //queue.remove(head);
                    publishProgress(head.getMessage());
                    //Log.i(TAG, "After publish progress Q size " + queue.size());
                    queue.remove(head);
                }

            }
        }*/

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            //Log.i(TAG , "Inside onProgess Update, Message String Received  - " + strReceived);
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
                id++;
                //Log.i(TAG, "About to insert in resolver : key  - " + Integer.toString(id) + " Value - " + strReceived);
                resolver.insert(mUri, values);

            } catch (Exception e){
                e.printStackTrace();
            }


            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs){

            try {
                counter++;
                //Log.i(TAG,"Inside Client task , counter = " + counter);
                String myPort = msgs[1].toString();
                ObjectOutputStream outgoing = null ;
                Socket socket = new Socket();


                //Log.i(TAG, "My port  - " + myPort);
                String msgToSend = msgs[0];

                for(String s : remote ) {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s));





                //Log.i(TAG , "Socket created at  " + socket.getPort() + "  " + socket.getLocalPort() + " Sending message - " + msgToSend);
                //if(socket!=null) {
                    //Log.i(TAG, "Before output stream " + s);
                    outgoing = new ObjectOutputStream(socket.getOutputStream());
                    //Log.i(TAG, "After output stream " + s);
                    if(outgoing!=null){
                        msgToSend = msgToSend + splitOrg + counter + splitOrg + myPort;
                        //Log.i(TAG, "Sending Original Message " + msgToSend + "to " + s);
                        outgoing.writeObject(msgToSend);
                        outgoing.flush();
                        outgoing.close();
                        //Log.i(TAG, "Sent message from - " + myPort + "to " + s);
                        //Log.i(TAG, "Socket at " + socket.getPort() + " closed. ");
                        msgToSend = msgs[0];
                    }

                //}
                socket.close();
            }

            }
            catch (UnknownHostException e) {
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
                Log.e(TAG, "ClientTask Exception");
                e.printStackTrace();
            }

            return null;
        }
    }
}



