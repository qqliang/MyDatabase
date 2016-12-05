package com.database.pager;


/**
 * Created by zoe on 2016/12/2.
 */
public class Column {
    private String name ;
    private byte type;
    private byte constraint;           //取值见ColumnConstraint

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public byte getConstraint() {
        return this.constraint;
    }

    public void setConstraint(byte constraint) {
        this.constraint = constraint;
    }
}
