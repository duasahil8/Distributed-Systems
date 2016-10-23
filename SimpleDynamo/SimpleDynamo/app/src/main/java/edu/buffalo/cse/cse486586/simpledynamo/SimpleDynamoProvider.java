package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {

	private static String myPort;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private static final ArrayList<String> remote = new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
	private static final int SERVER_PORT = 10000;
	private static SQLiteDatabase sqLiteDatabase ;
	static final String DATABASE_NAME = "simpleDynamo" ;
	static final String TABLE_NAME = "messages";
	static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (key TEXT UNIQUE, value TEXT ) ;";
	private static final String splitTarget = "#target#";
	private static final String splitMsg = "#message#";
	private static final String splitKeyValue = "#keyValuePair#";
	private static final String isAlive = "#isAlive#";
	private static final String finalInsert = "#finalInsert#";
	private static final String splitBackup = "#backup#";
	private static final String searchSingleKey = "#qKey#";
	private static final String foundSingleKey = "#foundSingle#";
	private static final String searchQueryAll = "#qAll#";
	private static final String splitFail = "#fail#";
	private static final String splitRow = "#mapRow#";
	private static final String backAlive = "#backAlive#";
	private static final String whileAway = "#whileAway#";
	private static final String ringCompleted = "#InsRing#";
	private static final String neighSplit = "#neigh#";
	private static boolean recoveryComplete = false ;


	private static ArrayList<String> hashedRing = new ArrayList<String>();
	//private static ArrayList<String> failedNodes = new ArrayList<String>() ;
	private static HashMap<String,String> hashToNodes = new HashMap<String, String>() ;
	private static ArrayList<String> failBackup = new ArrayList<String>() ;
	private static String failedNode = "";
	private static String succ = "";
	private static String pred = "";
	private static String nextOfSucc = "";
	private static boolean foundKey = false ;
	private static boolean insertRingCompleted = false ;
	private static boolean starQueryRing = false ;
	private static String myHash = "";

	private static boolean resultsFromAllAlive = false ;
	private static String searchResults = new String() ;
	private static String searchResultsGlobal  = new String();
	private static String probFail = "";
	private static final String checkFailure = "#checkFail#";
	private static  HashMap<String,String> dbHashMap = new HashMap<String, String>();


	private static boolean qToken = false ;
	private static boolean iToken = false ;

	//private static int c = 1 ;


	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		//int count =  sqLiteDatabase.delete(TABLE_NAME, " key=? ", new String[]{selection});
		String dmsg = selection + "#deleteBroadcast#";
		String fail = "";
		dbHashMap.remove(selection);
		Log.i(TAG, "D || " + selection + " || DB Size - " + dbHashMap.size());

		for(String s : remote){
			if(!s.equals(myPort)){
				try{
					fail = s ;
					Socket socket_new = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s));
					ObjectOutputStream outgoing = new ObjectOutputStream(socket_new.getOutputStream());
					outgoing.writeObject(dmsg);
					outgoing.close();
					socket_new.close();
				}
				catch (IOException e){
					Log.i(TAG, "Failure to send delete msg at " + fail );
				}


			}
		}


		return 1;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		iToken = true;

		String key = values.getAsString("key");
		String value = values.getAsString("value");
		String kv = key + splitKeyValue + value;
		Log.i(TAG, " I || " + key + " || " + value);
		int c = identifyNodes(key);
		//Log.i(TAG, "Insert Request - " + myPort + " " + key+splitKeyValue+value );

		String targets = "";
		Collections.sort(hashedRing);
		String t1, t2, t3 ;
		if(c==3){
			t1 = hashToNodes.get(hashedRing.get(3));
			t2 = hashToNodes.get(hashedRing.get(4));
			t3 = hashToNodes.get(hashedRing.get(0));
			targets = t1+splitTarget+t2+splitTarget+t3;
		}
		else if(c==4){
			t1 = hashToNodes.get(hashedRing.get(4));
			t2 = hashToNodes.get(hashedRing.get(0));
			t3 = hashToNodes.get(hashedRing.get(1));
			targets = t1+splitTarget+t2+splitTarget+t3;
		}

		else{
			t1 = hashToNodes.get(hashedRing.get(c));
			t2 = hashToNodes.get(hashedRing.get(c+1));
			t3 = hashToNodes.get(hashedRing.get(c+2));
			targets = t1+splitTarget+t2+splitTarget+t3;
		}


		ArrayList<String> ts = new ArrayList<String>();
		ts.add(t1); ts.add(t2);ts.add(t3);

		for(String t : ts){
			try{
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(t));
				ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
				String finalInsMsg = kv + finalInsert;
				outgoing.writeObject(finalInsMsg);
				try{
					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					String ack = (String)inputStream.readObject();
				}
				catch (IOException e){

					Log.i(TAG, "Failure detected during insert -  " + t );
					failedNode = t;
				}
				outgoing.close();
				socket.close();

			} catch (Exception e){

			}

		}

		Log.i(TAG, " I || Return  " + key + " || " + value + " || DB Size - " + dbHashMap.size());
		iToken = false;
		return uri;
	}

	@Override
	public boolean onCreate() {
		//myPort
		//dbHashMap = new HashMap<String, String>();
		//serverTask

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
			Log.e(TAG, "On_Create || Can't create a ServerSocket");
			return false;
		}

		Context context = this.getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf(Integer.parseInt(portStr) * 2);
		Log.i(TAG, "On Create || DB Size  " + dbHashMap.size());


		for(String port : remote){
			try {

				int avd = Integer.parseInt(port)/2;
				String hashedPort = genHash(String.valueOf(avd));
				hashToNodes.put(hashedPort,port);
				hashedRing.add(hashedPort);
            /*if(port.equals(myPort)){
               myHash = hashedPort;
            }*/


			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		Collections.sort(hashedRing);


		int avd = Integer.parseInt(myPort)/2;
		int c = identifyNodes(String.valueOf(avd));
		if(c==4) {
			succ = hashToNodes.get(hashedRing.get(0));
			nextOfSucc = hashToNodes.get(hashedRing.get(1));
		}
		else{
			succ = hashToNodes.get(hashedRing.get(c+1));

			if(c==3){
				nextOfSucc = hashToNodes.get(hashedRing.get(0));
			}
			else {
				nextOfSucc = hashToNodes.get(hashedRing.get(c+2));
			}
		}


		if(c==0) pred = hashToNodes.get(hashedRing.get(4));
		else pred = hashToNodes.get(hashedRing.get(c-1));


		//Log.i(TAG, "Ring - " + hashedRing);
		//Log.i(TAG, "Nodes - " + hashToNodes.toString());
		Log.i(TAG, "Pred || My || Succ || SuccNext " + pred + " || " + " ||" + myPort +  " || " + succ + " || " + nextOfSucc);





		//clientTask - backAlive

		Log.i(TAG, "On_Create || Sending back alive message!");

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, backAlive);

		recoveryComplete = false ;
		while(!recoveryComplete){

		}

		Log.i(TAG, "On_Create || Recovery Complete || DB SIZE " + dbHashMap.size()  );



      /*//db
      //dbHashMap = new HashMap<String, String>();
      SQLiteOpenHelper dbHelper = new DataBaseHelper(getContext());
      sqLiteDatabase = dbHelper.getWritableDatabase();
      //Log.i( TAG , "Returning from OnCreate ");
      if(sqLiteDatabase!=null) {
         //sqLiteDatabase.execSQL(CREATE_TABLE);
         //Log.i(TAG, "Database -  " + sqLiteDatabase.isDatabaseIntegrityOk());
         Cursor cr = sqLiteDatabase.rawQuery("select * from " + TABLE_NAME, null);
         //Log.i(TAG, "On Create DB Count - " + cr.getCount());
         return true;
      }

      else
         return false;*/

		return  true;

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
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
									 String[] selectionArgs, String sortOrder) {
		//Log.i(TAG, "Query at " + myPort + " " + selection);
		qToken = true ;

      /*while(iToken&&!recoveryComplete){

      }*/

		Cursor cursor;
		String[] cols = {"key" , "value"};
		if ("@".equals(selection)){
			MatrixCursor matrixCursor = new MatrixCursor(cols);
			for (Map.Entry<String, String> entry : dbHashMap.entrySet()){
				String k = entry.getKey();
				String v = entry.getValue();
				String[] row = {k,v};
				matrixCursor.addRow(row);

			}
			Log.i(TAG, " Q || @  Return Size - " + matrixCursor.getCount() + " || "  + " || DB Size - " + dbHashMap.size());
			qToken = false;
			return matrixCursor;
		}



		else if ("*".equals(selection)){
			Log.i(TAG, " Q * || DB Size -  " + dbHashMap.size());
			String sG = "";

			MatrixCursor matrixCursor = new MatrixCursor(cols);


         /*
         new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,searchQueryAll);

         starQueryRing = false;

         while(!starQueryRing){

         }*/

			String searchAll = myPort + searchQueryAll;
			String gR = "";
			String failQuery = "";
			try {


				for(String s : remote){
					if(!s.equals(myPort)){
						failQuery = s ;
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s));
						ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
						outgoing.writeObject(searchAll);
						try{
							ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
							gR += (String) inputStream.readObject();
							inputStream.close();

						}
						catch (IOException e ){
							Log.i(TAG , "Failure detected in * Query  " + failQuery);
							failedNode = failQuery;
						}
						outgoing.close();
						socket.close();

					}
				}

			}
			catch (Exception e) {
			}



			searchResultsGlobal = gR ;
			starQueryRing = true ;



			String globalR = "";
			starQueryRing = false;
			globalR = sG + searchResultsGlobal;
			Log.i(TAG, "Ring Completed   -  "  + myPort);

			for (Map.Entry<String, String> entry : dbHashMap.entrySet()){
				String key = entry.getKey();
				String value = entry.getValue();
				String[] row = {key,value};
				matrixCursor.addRow(row);

			}

			String rows[] = globalR.split(splitRow);
			for(String r : rows){
				String k = r.split(splitKeyValue)[0];
				String v = r.split(splitKeyValue)[1];
				String[] row = {k,v};
				matrixCursor.addRow(row);
			}
			Log.i(TAG, " Q || *  Return Size - " + matrixCursor.getCount() + " || "  + " || DB Size - " + dbHashMap.size());
			qToken = false;
			return matrixCursor;

		}


		else{
			Log.i(TAG, " Q || " + selection +  " || DB Size - " + dbHashMap.size());
			String q = "select * from " + TABLE_NAME + " where key = ?";

			if(dbHashMap.containsKey(selection)){
				String v =  dbHashMap.get(selection);
				MatrixCursor matrixCursor = new MatrixCursor(cols);
				String[] row = {selection,v};
				matrixCursor.addRow(row);
				Log.i(TAG, " Q || Return Local  " + selection + " || v - " + v + " || DB Size - " + dbHashMap.size());
				qToken = false;
				return matrixCursor;
			}

			int c = identifyNodes(selection);
			String targetPorts = "";

			if(c==4){
				targetPorts = hashToNodes.get(hashedRing.get(c)) + splitTarget + hashToNodes.get(hashedRing.get(0));
			}
			else{
				targetPorts = hashToNodes.get(hashedRing.get(c)) + splitTarget+ hashToNodes.get(hashedRing.get(c+1));
			}



			String searchKey = selection + searchSingleKey + targetPorts;
			String searchMsg = selection + searchSingleKey + targetPorts;
			String targetOne = targetPorts.split(splitTarget)[0];
			String targetTwo = targetPorts.split(splitTarget)[1];
			searchResults="";
			try{

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetOne));
				ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
				outgoing.writeObject(searchMsg);
				try    {
					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					searchResults = (String)inputStream.readObject();
					outgoing.close();
					inputStream.close();
					socket.close();
				}
				catch (IOException e){
					Log.i(TAG, "Failure detected -  " + targetOne);
					failedNode = targetTwo;
					socket =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetTwo));
					outgoing = new ObjectOutputStream(socket.getOutputStream());
					outgoing.writeObject(searchMsg);
					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					searchResults = (String)inputStream.readObject();
					outgoing.close();
					inputStream.close();
					socket.close();

				}
			}catch (Exception e){



			}






         /*new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, searchKey);


         Log.i(TAG, " Q || Waiting " + selection + " || DB Size - " + dbHashMap.size());

         while (!foundKey) {

         }
         foundKey = false;*/
			//Log.i(TAG, "R  - " + searchResults );
			String k = searchResults.split(splitKeyValue)[0];
			String v = searchResults.split(splitKeyValue)[1];

			String[] row = {k, v};
			MatrixCursor mcrs = new MatrixCursor(cols);
			mcrs.addRow(row);
			Log.i(TAG, " Q || Return  " + k + " || " + v + " || DB Size - " + dbHashMap.size());

			//assign key , value to crs
			qToken = false;
			return mcrs;
		}


	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		private Socket connection ;
		private ObjectInputStream incoming ;
		private ObjectOutputStream outgoing ;

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			//Log.i(TAG, "Server Task Thread created ...") ;
			try {
				ServerSocket serverSocket = sockets[0];

				while (true) {

					connection = serverSocket.accept();
					incoming = new ObjectInputStream(connection.getInputStream());
					String message = (String) incoming.readObject();

					while(!recoveryComplete){

					}

					if (message.contains(finalInsert)){
						String kv = message.split(finalInsert)[0];
						String key = kv.split(splitKeyValue)[0];
						String value = kv.split(splitKeyValue)[1];
						dbHashMap.put(key, value);
						outgoing = new ObjectOutputStream(connection.getOutputStream());
						outgoing.writeObject("#insertAck#");
						outgoing.close();
						incoming.close();
					}

					if (message.contains("#deleteBroadcast#")){
						String rem = "";
						rem = message.split("#deleteBroadcast#")[0];
						Log.i(TAG , " Delete broadcast received - " + rem);
						dbHashMap.remove(rem);
						incoming.close();

					}


					if(message.contains(backAlive)){

						String allDB = "";
						if(dbHashMap.size()>0){

							for (Map.Entry<String, String> entry : dbHashMap.entrySet()){
								String key = entry.getKey();
								String value = entry.getValue();
								String[] row = {key,value};
								allDB += key + splitKeyValue + value + splitRow;
							}
						}

						outgoing = new ObjectOutputStream(connection.getOutputStream());
						outgoing.writeObject(allDB);
						outgoing.close();
						incoming.close();

					}


					if(message.contains(searchQueryAll)){

						String req = message.split(searchQueryAll)[0];

						String next = succ;

						String sGlobal = "";
						for (Map.Entry<String, String> entry : dbHashMap.entrySet()){
							String key = entry.getKey();
							String value = entry.getValue();
							sGlobal+=key+splitKeyValue+value+splitRow;

						}

						outgoing = new ObjectOutputStream(connection.getOutputStream());
						outgoing.writeObject(sGlobal);
						outgoing.close();
						incoming.close();
					}

					if(message.contains(searchSingleKey)){
						String key = message.split(searchSingleKey)[0];
						String v =  dbHashMap.get(key);
						String result = key + splitKeyValue + v;
						outgoing = new ObjectOutputStream(connection.getOutputStream());
						outgoing.writeObject(result);
						outgoing.close();
						incoming.close();
					}


					connection.close();
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
			//(TAG , "Inside onProgess Update, Message String Received  - " + strReceived);


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

	private synchronized void whileAwayHandler(String message) {

		String db = message ;
		int avd = Integer.parseInt(myPort)/2;
		int count = 0 ;
		int myPosition = identifyNodes(String.valueOf(avd));
		//Log.i(TAG, "Filtering Received DB! My position in Ring - " + myPosition + " || " + myPort);
		if(db.contains(splitRow)){
			String[] rows = db.split(splitRow);
			Log.i(TAG,"Filter  || " + myPort + " || Received Rows - " + rows.length);
			for(String r : rows){
				String key = r.split(splitKeyValue)[0];
				String value = r.split(splitKeyValue)[1];
				int c = identifyNodes(key);
				boolean keepKey = false ;
				if(myPosition==0){
					if(c==3||c==4||c==0){
						keepKey = true ;
					}
				}
				else if(myPosition==1){
					if(c==0||c==4||c==1){
						keepKey = true ;
					}
				}
				else{
					if(c==myPosition-1||c==myPosition-2||c==myPosition){
						keepKey = true ;
					}
				}
				//Log.i(TAG, "Key - " + key + " || Keep - " + keepKey);
				if(keepKey){
					count++;
					dbHashMap.put(key, value);
					//Log.i(TAG, "Recovery insert - || " + key + " || " + value);

				}

			}
			Log.i(TAG,"Filter  || " + myPort + " || Inserted  Rows - " + count);
		}

	}

	private class ClientTask extends AsyncTask<String, Void, Void> {


		@Override
		protected Void doInBackground(String... msgs) {
			Socket socket;
			String msg = msgs[0];


			try {

				if(msg.contains(splitMsg)){

					String targets = msg.split(splitMsg)[0];
					String kv = msg.split(splitMsg)[1];

					String t1 = targets.split(splitTarget)[0];
					String t2 = targets.split(splitTarget)[1];
					String t3 = targets.split(splitTarget)[2];

					ArrayList<String> ts = new ArrayList<String>();
					ts.add(t1); ts.add(t2);ts.add(t3);

					for(String t : ts){

						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(t));
						ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
						String finalInsMsg = kv + finalInsert;
						outgoing.writeObject(finalInsMsg);
						try{
							ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
							String ack = (String)inputStream.readObject();
						}
						catch (IOException e){

							Log.i(TAG, "Failure detected during insert -  " + t );
							failedNode = t;
						}
						outgoing.close();
						socket.close();

					}

					insertRingCompleted = true;

				}



				//back Alive Msg
				if(msg.contains(backAlive)){
					Log.i(TAG , "Back Alive!!  -  " + myPort);
					String backMsg = myPort+backAlive;
					String allDb = "";
					for(String s : remote){
						if(!s.equals(myPort)){
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s));
							ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
							outgoing.writeObject(backAlive);

							try{
								ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
								String reply = (String)inputStream.readObject();
								allDb = allDb + reply;
							}
							catch (IOException e ){

							}

							outgoing.close();
							socket.close();
						}

					}

					whileAwayHandler(allDb);
					recoveryComplete = true ;
				}

				//client - single query
				if(msg.contains(searchSingleKey)){

					String ports= msg.split(searchSingleKey)[1];
					String key = msg.split(searchSingleKey)[0];
					String targetOne = ports.split(splitTarget)[0];
					String targetTwo = ports.split(splitTarget)[1];
					//String searchMsg = key+searchSingleKey+myPort;
					String searchMsg = key+searchSingleKey;

					try{
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetOne));
						ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
						outgoing.writeObject(searchMsg);
						ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
						searchResults = (String)inputStream.readObject();
						outgoing.close();
						inputStream.close();
						socket.close();
					}
					catch (IOException e){
						Log.i(TAG, "Failure detected -  " + targetOne);
						failedNode = targetTwo;

						socket =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetTwo));
						ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
						outgoing.writeObject(searchMsg);
						ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
						searchResults = (String)inputStream.readObject();
						outgoing.close();
						inputStream.close();
						socket.close();

					}


					foundKey = true ;

				}

				//query *  //client - ring forward
				if(msg.contains(searchQueryAll)){

					String searchAll = myPort + searchQueryAll;
					String gR = "";
					String failQuery = "";

					for(String s : remote){
						if(!s.equals(myPort)){
							failQuery = s ;
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s));
							ObjectOutputStream outgoing = new ObjectOutputStream(socket.getOutputStream());
							outgoing.writeObject(searchAll);
							try{
								ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
								gR += (String) inputStream.readObject();
								inputStream.close();

							}
							catch (IOException e ){
								Log.i(TAG , "Failure detected in * Query  " + failQuery);
								failedNode = failQuery;
							}
							outgoing.close();
							socket.close();

						}
					}

					searchResultsGlobal = gR ;
					starQueryRing = true ;


				}



			} catch (IOException e) {
				Log.e(TAG, "ClientTask General  IOException - " + probFail + " || My Port - " + myPort);
            /*if(!failedNodes.contains(probFail)&&!probFail.equals(""))
               failedNodes.add(probFail);*/
				e.printStackTrace();
			}
			catch (NullPointerException e) {
				Log.e(TAG, "ClientTask socket NUllPointerException");
				e.printStackTrace();
			}
			catch (Exception e) {
				Log.e(TAG, "ClientTask General Exception");
				e.printStackTrace();
			}

			return null;
		}
	}

	private int identifyNodes(String k){
		int i = 0 ;
		int c = -1 ;
		int nodes = hashedRing.size();
		Collections.sort(hashedRing);
		String key = null;
		try {
			key = genHash(k);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		//Log.i(TAG,"Inside identify nodes  - " + nodes);
		for(i=0; i<4;i++) {
			int keyPort = key.compareTo(hashedRing.get(i));
			int keyPortNext = key.compareTo(hashedRing.get(i + 1));
			if (i == 0) {
				int keyLast = key.compareTo(hashedRing.get(4));
				if(keyPort>0&&keyPortNext<=0)
					c = i+1 ;
				else if((keyLast>0&&keyPort>0)||(keyLast<0&&keyPort<=0)){
					c = 0 ;
				}
			}

			else if(keyPort>0&&keyPortNext<=0)
				c = i+1 ;
		}
		return c;

	}

}