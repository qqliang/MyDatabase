package com.database.global;

/**
 * Created by zoe on 2016/12/8.
 */
public class PageType {
    public static final byte TABLE_LEAF = 0x03;         //3
    public static final byte TABLE_INTERNAL = 0x02;     //2
    public static final byte TABLE_ROOT = 0x01;         //1
    public static final byte TABLE_OVERFLOW = 0x04;     //4
}
