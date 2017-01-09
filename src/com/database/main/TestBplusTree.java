package com.database.main;

import com.database.global.Database;
import com.database.myBplusTree.BplusTree;
import com.database.pager.Pager;
import com.database.pager.TableSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Qing_L on 2016/12/9.
 */
public class TestBplusTree {
    public static void main(String args[]){

        BplusTree tree = new BplusTree(3, new Database(),new TableSchema());

        List<String> list = new ArrayList<>();
        list.add("1,lqq,87");list.add("2,zy,56");list.add("3,hh,46");list.add("4,cy,87");
        list.add("5,cq,72");list.add("6,gf,45");list.add("7,dg,76");list.add("8,hg,54");

        for(int i=0;i<list.size();i++){
            tree.Insert(list.get(i));
        }
    }
}
