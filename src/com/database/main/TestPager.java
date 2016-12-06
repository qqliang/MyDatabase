package com.database.main;

import com.database.global.ColumnConstraint;
import com.database.global.DataType;
import com.database.global.Database;
import com.database.pager.Column;
import com.database.pager.Pager;
import com.database.pager.Record;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zoe on 2016/12/5.
 */
public class TestPager {
    public static void main(String[] args){
//        testRead();
        testFlush();
    }
    public static void testFlush(){
        Database database = new Database();
        database.openDB("test2");
        Pager pager = new Pager(database);
        Record record = getRecord();

        pager.writeData(1,record.getBytes("1,zhouyu,22"));
        pager.writeData(1,record.getBytes("2,lqq,22"));
        pager.writeData(1,record.getBytes("3,hh,26"));

        pager.writeData(1,record.getBytes("4,dxr,27"));
        pager.writeData(1,record.getBytes("5,yyc,27"));
        pager.writeData(1,record.getBytes("6,whw,22"));

        pager.flush();

    }
    public static void testRead(){
        Database database = new Database();
        database.openDB("test");
        Pager pager = new Pager(database);
        Record record = getRecord();

        pager.writeData(1,record.getBytes("1,zhouyu,22"));
        pager.writeData(1,record.getBytes("2,lqq,22"));
        pager.writeData(1,record.getBytes("3,hh,26"));

        List<String> list = pager.readRecord(1);
        System.out.println(list.toString());
    }
    public static void testAdd(){
        Database database = new Database();
        database.openDB("test");
        Pager pager = new Pager(database);
        Record record = getRecord();

        pager.writeData(1,record.getBytes("1,zhouyu,22"));
        pager.writeData(1,record.getBytes("2,lqq,22"));
        pager.writeData(1,record.getBytes("3,hh,26"));
    }
    public static Record getRecord() {
        Record record = new Record();
        List<Column> cols = new ArrayList<Column>();
        Column idCol =  new Column();
        idCol.setName("id");
        idCol.setType(DataType.INTEGER);
        idCol.setConstraint(ColumnConstraint.NONE);

        Column nameCol =  new Column();
        nameCol.setName("name");
        nameCol.setType(DataType.TEXT);
        nameCol.setConstraint(ColumnConstraint.NONE);

        Column ageCol =  new Column();
        ageCol.setName("age");
        ageCol.setType(DataType.TINY_INT);
        ageCol.setConstraint(ColumnConstraint.NONE);

        cols.add(idCol);
        cols.add(nameCol);
        cols.add(ageCol);
         record.setColumns(cols);
        return record;
    }
}
