package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by ramya on 5/1/15.
 */
public class SimpleDynamoOpenHelper extends SQLiteOpenHelper {

    private static final int DATABASE_VERSION = 1;
    private static final String SIMPLE_DYNAMO_TABLE_CREATE =
            "CREATE TABLE simple_dynamo (key TEXT PRIMARY KEY, value TEXT);";

    public SimpleDynamoOpenHelper(Context context, String name,
                               SQLiteDatabase.CursorFactory factory, int version) {
        super(context, "simple_dynamo", factory, DATABASE_VERSION);
    }
    @Override
    public void onUpgrade(SQLiteDatabase db,int oldVersion, int newVersion) {
        // dropping an older version of the table , if it exists
        Log.v("db onUpgrade", "simple_dynamo");
        db.execSQL("DROP TABLE IF EXISTS simple_dynamo");
        onCreate(db);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS simple_dynamo");
        db.execSQL(SIMPLE_DYNAMO_TABLE_CREATE);
        Log.v("db onCreate","simple_dynamo");
    }
}
