package com.database.pager;

import com.database.global.ColumnConstraint;
import com.database.global.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by zoe on 2016/12/5.
 */
public class Record {
    private List<Column> columns;
    private int size ;
    private int colNum;

    public Record(List<Column> columns) {
        this.columns = columns;
        this.size = calculateSize();
        this.colNum = columns.size();
    }

    public Record() {
    }

    public List<Column> getColumns() {
        return columns;
    }

    /**
     *  对一条记录中的行信息进行设置
     * @param columns   要设置的行信息列表
     */
    public void setColumns(List<Column> columns) {
        this.columns = columns;
        this.size = calculateSize();
    }

    /**
     * 对一条记录中的行信息进行设置
     * @param recordStr     一条记录的字符串，每个字段以逗号分隔。格式为：xxx,yyy,zzz......
     * @param type          与recordStr每个字段对应的类型
     * @param constraints   与每个字段对应的字段约束，可以为NULL
     */
    public void setColumns(String recordStr, List<Byte> type, List<Byte> constraints) {
        String[] cols = recordStr.split(",");

        assert(cols.length == type.size());

        if(constraints == null || constraints.size() == 0) {
            constraints = new ArrayList<Byte>(cols.length);
            Collections.fill(constraints, ColumnConstraint.NONE);
        }else if(constraints.size() < cols.length){
            List<Byte> newConstraints = new ArrayList<Byte>(cols.length);
            for(int i = 0; i < cols.length; i++){
                if(i < constraints.size())
                    newConstraints.set(i,constraints.get(i));
                else
                    newConstraints.set(i,ColumnConstraint.NONE);
            }
        }

        if(this.columns != null && this.columns.size()!=0)
            this.columns.clear();

        for(int i =0; i<cols.length; i++){
            Column column = new Column();
            column.setName(cols[i]);
            column.setType(type.get(i));
            column.setConstraint(ColumnConstraint.NONE);

            this.columns.add(column);
        }

        this.size = calculateSize();
    }
    public int calculateSize(){
        if(this.columns == null || this.columns.size() == 0)
            return 0;

        int size = 0 ;

        for(int i = 0; i < colNum; i++){
            switch (this.columns.get(i).getType()){
               case DataType.INTEGER :
                   size += 4;
               case DataType.LONG :
                   size += 8;
               case DataType.TEXT:
                   size += 50;
                case DataType.SMALL_INT:
                    size += 2;
                case DataType.TINY_INT:
                    size += 1;
            }
        }

        return size;
    }

    public int getSize(){
        return  this.size;
    }
    public int getColNum(){
        return this.columns.size();
    }
}
