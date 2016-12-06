package com.database.pager;

import java.util.List;

/**
 * Created by zoe on 2016/12/2.
 */
public class Table {
    private String name;
    private Column column;              //列信息（只包含列的定义,不包含数据）
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }
}
