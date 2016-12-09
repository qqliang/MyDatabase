package com.database.main;

import com.database.global.ColumnConstraint;
import com.database.global.DataType;
import com.database.pager.Column;
import com.database.pager.TableSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zoe on 2016/12/5.
 */
public class TestRecord {
    public static void main(String[] args){
        testCalculateSize();
    }
    public static void testCalculateSize(){
        TableSchema record = getRecord();
        int size = record.calculateSize();
        System.out.println(size);
    }
    public static TableSchema getRecord(){
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
