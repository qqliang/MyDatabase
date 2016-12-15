package com.database.parse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import com.database.global.ColumnConstraint;
import com.database.global.DataType;
import com.database.pager.Column;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;


public class parser {

    static String results[] = new String[5];


    static Statement statement = null;

    /**
     *  判断字段名是否唯一
     * @param str
     * @return
     */
    public static boolean isUnique(String str[]){
        boolean flag=true;
        for(int i=0;i<str.length;i++){
            String temp=str[i];
            for(int j=i+1;j<str.length;j++){
                if(temp.equals(str[j])){
                    flag=false;
                    break;
                }
            }
        }
        return flag;
    }

    /**
     *  输入建表 sql 得到表结构
     * @param sql
     * @return
     */
    public List<Column> getTableSchema(String sql){
        List<Column> columns=new ArrayList<Column>();
        Column column =new Column();
        CCJSqlParserManager pm = new CCJSqlParserManager();
        try{
            statement = pm.parse(new StringReader(sql));
        }catch (Exception e){
            System.out.println("非标准SQL");
        }
        if (statement instanceof CreateTable) {
            CreateTable create = (CreateTable) statement;
            List<ColumnDefinition> colDef = create.getColumnDefinitions();
            if( colDef ==null){
                System.out.println("非标准SQL");
            }else {
                String cols[]=new String[colDef.size()];
                int i=0;
                for(ColumnDefinition def : colDef){
                    cols[i++]=def.getColumnName();
                }
                if(isUnique(cols)){        // 检查 表结构 字段是否 有重复
                    for(ColumnDefinition def : colDef){
                        column.setName(def.getColumnName());    // 字段名
                        String type=def.getColDataType().getDataType();  // 字段类型
                        DataType dataType=new DataType();
                        if(type.equals("int") || type.equals("integer")){
                            column.setType(dataType.INTEGER);
                        }else if(type.equals("tingint") || type.equals("tinginteger")){
                            column.setType(dataType.TINY_INT);
                        }else if(type.equals("smallint") || type.equals("smallinteger")){
                            column.setType(dataType.SMALL_INT);
                        }else if(type.equals("long") || type.equals("bigint") || type.equals("biginteger")){
                            column.setType(dataType.LONG);
                        }else if(type.equals("char") || type.equals("varchar") || type.equals("text")){
                            column.setType(dataType.TEXT);
                        }
                        String constraint =def.getColumnSpecStrings().toString();   // 约束
                        ColumnConstraint columnConstraint = new ColumnConstraint();
                        if(constraint.equals("primary key") || constraint.equals("PRIMARY KEY")){
                            column.setConstraint(columnConstraint.PRIMARY_KEY );
                        }else if(constraint.equals("not null") || constraint.equals("NOT NULL")){
                            column.setConstraint(columnConstraint.NOT_NULL );
                        }else if(constraint.equals("foreign key") || constraint.equals("FOREIGN KEY")){
                            column.setConstraint(columnConstraint.FOREIGN_KEY );
                        }
                        columns.add(column);
                    }
                }else{
                    System.out.println("非标准SQL");
                }
            }
        }else{
            System.out.println("非建表SQL");
        }
        return columns;
    }


    public static void parserCRUD(String sql){
        CCJSqlParserManager pm = new CCJSqlParserManager();
        try{
            statement = pm.parse(new StringReader(sql));
        }catch (Exception e){
            System.out.println("非标准SQL");
            results[0]="ERROR";
        }

        if (statement instanceof CreateTable) {
            CreateTable create = (CreateTable) statement;
            List<ColumnDefinition> colDef = create.getColumnDefinitions();
            if( colDef ==null){
                System.out.println("非标准SQL");
                results[0]="ERROR";
            }else {
                String cols[]=new String[colDef.size()];
                int i=0;
                for(ColumnDefinition def : colDef){
                    cols[i++]=def.getColumnName();
                }
                if(isUnique(cols)){
                    results[0]="10";
                    results[1]=create.getTable().getName(); //表名
                    results[2]=colDef.toString();   // 表结构
                }else{
                    System.out.println("非标准SQL");
                    results[0]="ERROR";
                }
            }
        }

        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            results[0]="22";
            results[1]=insert.getTable().getName(); //表名
            results[2]=insert.getItemsList().toString();  // values  值
        }

        if (statement instanceof Select) {
            Select select = (Select) statement;
            PlainSelect plain =(PlainSelect) select.getSelectBody();
            results[0] = "21";
            results[1] = plain.getSelectItems().toString();  // 查找 字段
            results[2] = plain.getFromItem().toString();   // 表名
            if(plain.getWhere() !=null){
                results[3] = plain.getWhere().toString();	// 查找条件
            }
        }

        if (statement instanceof Delete) {
            Delete delete = (Delete) statement;
            results[0] = "23";
            results[1] = delete.getTable().getName();  //表名
            if(delete.getWhere() !=null){
                results[2] = delete.getWhere().toString();  // 删除条件
            }
        }


    }

    public static String[] parser(String str) {
        String strOri=str.replaceAll(" ", "");  // 去除所有空格
        if(strOri.length()>=15){
            String sql []=new String[3];
            sql[0]=strOri.substring(0,6);	// create
            sql[1]=strOri.substring(6,14);  // database
            sql[2]=strOri.substring(14,strOri.length());  // 数据库名称
            if(sql[0].toUpperCase().equals("CREATE") && sql[1].toUpperCase().equals("DATABASE")){
                results[0] = "11"; // database 11
                results[1] = sql[2];  //数据库名称
            }else{
                parserCRUD(str);
            }
        }else{
            System.out.println("非标准SQL");
            results[0]="ERROR";
        }

        return results;
    }

    /*public static void main(String[] args){
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String str="";
        try {
            str = in.readLine();
            //System.out.println(sql);
        } catch (IOException e) {

            e.printStackTrace();
        }

        String strOri=str.replaceAll(" ", "");  // 去除所有空格
        if(strOri.length()>=15){
            String sql []=new String[3];
            sql[0]=strOri.substring(0,6);	// create
            sql[1]=strOri.substring(6,14);  // database
            sql[2]=strOri.substring(14,strOri.length());  // 数据库名称
            //String sql[] = StringToSql(str);
            if(sql[0].toUpperCase().equals("CREATE") && sql[1].toUpperCase().equals("DATABASE")){
                results[0] = "11"; // database 11
                results[1] = sql[2];
            }else{
                //System.out.println("非标准SQL");
                parser(str);
            }
        }else{
            System.out.println("非标准SQL");
        }
        for(int i=0;i<results.length;i++){
            System.out.println(results[i]);
        }
    }*/
}