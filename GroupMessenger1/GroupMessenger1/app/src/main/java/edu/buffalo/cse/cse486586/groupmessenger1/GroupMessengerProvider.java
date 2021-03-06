package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

import java.util.HashMap;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 *
 * Please read:
 *
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 *
 * before you start to get yourself familiarized with ContentProvider.
 *
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 *
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    static final String TAG = GroupMessengerProvider.class.getSimpleName();


   /* static {

        uriMatcher.addURI(PROVIDER_NAME,"provider", uriCode);
    }*/

    private static SQLiteDatabase sqLiteDatabase ;
    static final String DATABASE_NAME = "groupMessenger" ;
    static final String TABLE_NAME = "messages";
    static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (key TEXT, value TEXT );";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */
        Log.i(TAG, "Inside insert. Host -  " + uri.getHost() + " Values - " + values.valueSet().toString());

        long rowId  = sqLiteDatabase.insert(TABLE_NAME, null, values);
        Log.i(TAG, "Row id -  " + rowId + " URI -  " + uri.toString() );
        Log.v("insert", values.toString());

        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        Log.i(TAG, "Inside on create of Provider  ");
        SQLiteOpenHelper  dbHelper = new DataBaseHelper(getContext());
        sqLiteDatabase = dbHelper.getWritableDatabase();


        if(sqLiteDatabase!=null) {
            //sqLiteDatabase.execSQL(CREATE_TABLE);
            Log.i(TAG, "Database -  " + sqLiteDatabase.isDatabaseIntegrityOk());
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
            Log.i(TAG, "Inside on create of private sub class of SQLiteOpenHelper");
            db.execSQL(CREATE_TABLE);
            //Log.i(TAG, "After creating table " + sqLiteDatabase.getAttachedDbs().toString());
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        }
    }




    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        SQLiteQueryBuilder queryBuilder  = new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);
        // String query = "select key,value from " + TABLE_NAME + " where key = ? LIMIT 1";

        Cursor cursor =
                sqLiteDatabase.rawQuery("select key,value from " + TABLE_NAME + " where key = ? limit 1", new String[]{selection});

        Log.i(TAG , Integer.toString(cursor.getCount()));

        Log.v("query", selection);
        Log.i(TAG, "Cursor - " + cursor.getColumnNames().toString());
        if(cursor!=null)return cursor;
        else
            return null;
    }


}
