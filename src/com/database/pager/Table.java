package com.database.pager;

/**
 * Created by zoe on 2016/12/2.
 */
public class Table {
    private String name;
    private Column[] column;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Column[] getColumn() {
        return column;
    }

    public void setColumn(Column[] column) {
        this.column = column;
    }
}
