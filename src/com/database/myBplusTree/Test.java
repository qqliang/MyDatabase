//package com.database.myBplusTree;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Created by Qing_L on 2016/11/24.
// */
//public class Test {
//    // 测试
//    public static void main(String[] args) {
//
//        Integer[] id = new Integer[12];
//        id[0]=6;id[1]=1;id[2]=2;id[3]=12;id[4]=5;id[5]=3;
//        id[6]=4;id[7]=11;id[8]=8;id[9]=10;id[10]=7;id[11]=9;
//
//        List<String> value = new ArrayList<>();
//        value.add("周彧,97");value.add("陈琦,86");value.add("侯浩,76");value.add("陈扬,98");
//        value.add("户庆凯,78");value.add("吴小全,85");value.add("梁青青,83");value.add("丁玺润,71");
//        value.add("杨庆,90");value.add("周耀,80");value.add("闵胜天,82");value.add("张杰,72");
//
//        int order = 3;
//
//        BplusTree bplusTree = new BplusTree(order);
//
//        //插入
//        bplusTree.Insert(id,value);
//
//        //查询
//        System.out.println(bplusTree.Search(5));
//
//        //删除
//       System.out.println(bplusTree.Remove(4));
//    }
//
//
//}
