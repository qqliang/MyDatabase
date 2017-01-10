package com.database.main;

import com.database.global.Database;
import com.database.myBplusTree.BplusTree;
import com.database.pager.Pager;
import com.database.pager.TableSchema;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Qing_L on 2016/12/9.
 */
public class TestBplusTree {
    public static void main(String args[]){

        Database db = new Database();//数据库对象
        String sql ;

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        int rc = db.openDB("testDB");
        if(rc==0){
            System.out.println("数据库不存在");
            return;
        }
        BplusTree tree = db.getTableTreeByName("test");

        List<String> list = new ArrayList<>();
//        list.add("1,lqq");list.add("2,zy");list.add("3,hh");
//        list.add("4,cy");
//        list.add("5,cq");
        list.add("6,gf");
//        list.add("7,dg");list.add("8,hg");

//        List<String> resultList = new ArrayList<>();
//        for(int i=0;i<list.size();i++){
//            String result = tree.SelectByKey(i);
//            resultList.add(result);
//            System.out.println("查询结果：" + result);
//        }
        for(int i=0;i<list.size();i++){
            tree.Insert(list.get(i));
        }
    }
}
