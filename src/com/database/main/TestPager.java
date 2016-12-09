package com.database.main;

import com.database.global.ColumnConstraint;
import com.database.global.DataType;
import com.database.global.Database;
import com.database.global.PageType;
import com.database.pager.Column;
import com.database.pager.Pager;
import com.database.pager.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zoe on 2016/12/5.
 */
public class TestPager {
    private static Database database ;
    private static TableSchema schema;
    private static Pager pager ;

    /**
     * 对类对象进行初始化
     */
    public  static void init(){
        database = new Database();
        database.openDB("test2");
        pager  = new Pager(database);
        schema = getSchema();
    }

    public static void main(String[] args){
        init();
//        testRead();
//        testReadByRowid();
//        testFlush();
        testLoad();
    }

    /**
     *  测试数据刷新到磁盘
     */
    public static void testFlush(){

        int rowid = 0;
        pager.writeData(1, schema.getBytes(++rowid, "1,zhouyu,22"));
        pager.writeData(1, schema.getBytes(++rowid,"2,lqq,22"));
        pager.writeData(1, schema.getBytes(++rowid,"3,hh,26"));

        pager.writeData(1, schema.getBytes(++rowid,"4,dxr,27"));
        pager.writeData(1, schema.getBytes(++rowid,"5,yyc,27"));
        pager.writeData(1, schema.getBytes(++rowid,"6,whw,22"));
        pager.flush();

    }

    /**
     * 测试数据是否能从数据库文件正确的加载
     */
    public static void testLoad()
    {
        Database database = new Database();
        database.openDB("test2");
        Pager pager = new Pager(database);
        pager.loadDB();

        List<Map.Entry<Integer, String>> list = pager.readRecord(1);
        System.out.println(list);
    }

    /**
     * 测试是否能读取页面数据
     */
    public static void testRead(){

        int rowid = 0;
        pager.writeData(1,schema.getBytes(rowid++,"1,zhouyu,22"));
        pager.writeData(1,schema.getBytes(rowid++,"2,lqq,22"));
        pager.writeData(1,schema.getBytes(rowid++,"3,hh,26"));

        List<Map.Entry<Integer, String>> list = pager.readRecord(1);
        System.out.println(list.toString());
    }

    /**
     * 测试通过rowid对某一条目的查找
     */
    public static void testReadByRowid(){

        pager.getPages()[1].setPageType(PageType.TABLE_LEAF);
        int rowid = 0;
        pager.writeData(1,schema.getBytes(rowid++,"1,zhouyu,22"));
        pager.writeData(1,schema.getBytes(rowid++,"2,lqq,22"));
        pager.writeData(1,schema.getBytes(rowid++,"3,hh,26"));

        String row = pager.readDataByRowid(1,2).getValue();
        System.out.println("row:"+row);
    }

    /**
     *  自己构造的模式对象
     * @return
     */
    public static TableSchema getSchema() {
        TableSchema record = new TableSchema();
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
