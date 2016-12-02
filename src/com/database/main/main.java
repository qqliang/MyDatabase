package com.database.main;

import com.database.global.Databases;

/**
 * Created by Qing_L on 2016/12/2.
 */
public class main {
    public static void main(String args[]){
        Databases db = new Databases();

        String sql = "create database testDB";
//        db.exeSQL(sql);

        int rc = db.openDB("testDB");
        if(rc==0){
            System.out.println("数据库不存在");
            return;
        }

        sql = "create table test (id int, name char)";
        db.exeSQL(sql);

        sql = "insert into test values (2,'zy')";
        db.exeSQL(sql);

//        sql = "select * from test";
//        db.exeSQL(sql);
    }
}
