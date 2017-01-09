package com.database.main;


import com.database.global.Utils;

import java.util.Arrays;
import java.util.Random;

/**
 * Created by zoe on 2016/12/5.
 */
public class TestUtils {
    public static void main(String[] args){
//        testInt();
//        testLong();
//        testString();
//        testShort();
        testVarInt();
    }
    public static void testShort(){
        int max = 2 << 15 -10;
        int min = 2 << 8;
        Random random = new Random();
        int old = random.nextInt(max)%(max-min+1) + min;
        byte[] data = new byte[1024];
        Arrays.fill(data,(byte)0);
        Utils.fillShort((short)old,data,0);
        short value = Utils.loadShortFromBytes(data,0);
        System.out.println("short value:"+(old == value));
    }
    public static void testInt(){
        int max = 2 << 31 -10;
        int min = 2 << 15 -10;
        Random random = new Random();
        int old = random.nextInt(max)%(max-min+1) + min;
        byte[] data = new byte[1024];
        Arrays.fill(data,(byte)0);
        Utils.fillInt(old,data,0);
        int value = Utils.loadIntFromBytes(data,0);
        System.out.println("int value:"+(old == value));
    }
    public static void testLong(){
        Random random = new Random();
        long old = random.nextLong();
        byte[] data = new byte[1024];
        Arrays.fill(data,(byte)0);
        Utils.fillLong(old,data,0);
        long value = Utils.loadLongFromBytes(data,0);
        System.out.println("long value:"+ new Long(old).toString().equals(new Long(value).toString()));
    }
    public static void testString(){
        byte[] data = new byte[1024];
        String old = "zhouyu";
        Arrays.fill(data,(byte)0);
        Utils.fillString(old,data,0);
        String value = Utils.loadStrFromBytes(data,0,new String("zhouyu").length());
        System.out.println("String value:"+(old.equals(value)));
    }

    /**
     * 测试对变长数据的存储于读取功能
     */
    public static void testVarInt(){

        byte[] data = new byte[5];
        int oldValue = 0xffffffff;
        System.out.println("byte value:"+Integer.toBinaryString(oldValue));
        int len = Utils.fillVarInt(oldValue, data, 0);
        System.out.println("len:"+ len);
        int newValue = Utils.loadVarIntFromBytes(data, 0);
        System.out.println("data:"+Arrays.toString(data));
        System.out.println("old == new:"+ (oldValue==newValue));

    }

}
