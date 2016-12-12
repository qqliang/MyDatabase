package com.database.pager;

/**
 * Created by zoe on 2016/12/8.
 */
public class Position {
    public static int PGNO_IN_PAGE = 0 ;
    public static byte PGTYPE_IN_PAGE = 4;
    public static int OFFSET_IN_PAGE = 5;
    public static int OVERFLOWPGNO_IN_PAGE = 9;
    public static int PARENT_PAGE_IN_PAGE = 13;
    public static int PREV_PAGE_IN_PAGE = 17;
    public static int NEXT_PAGE_IN_PAGE = 21;
    public static byte ORDER_IN_BPLUS_ROOT = 25;
    public static int HEAD_IN_BPLUS_ROOT = 26;
    public static int MAX_ROWID_IN_BPLIS_ROOT = 30;
    public static byte CELLNUM_IN_PAGE = 34;
    public static int CELL_IN_PAGE = 35;

    public static int ROWID_IN_RECORD = 0;
    public static int HEADER_IN_RECORD = 4;


    //page 1 header
    public static int TABLE_COUNT_IN_FIRST_PAGE = 120;
}
