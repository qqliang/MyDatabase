package com.database.main;

import com.database.global.Database;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Qing_L on 2016/12/2.
 */
public class main {
    public static void main(String args[]){

        Database db = new Database();//数据库对象
        String sql ;

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        int rc = db.openDB("testDB");
        if(rc==0){
            System.out.println("数据库不存在");
            return;
        }
        try {
            System.out.println("请输入SQL语句");
            sql = br.readLine();
            while ( !sql.equals("quit")){
                db.exeSQL(sql);
                sql = br.readLine();
            }
            System.out.println("已退出！");
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
