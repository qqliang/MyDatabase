package com.database.parse;

import java.io.StringReader;
import java.util.List;
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

    public static String[] StringToSql(String str) {
        String sql[] = str.split(" ");
        return sql;
    }

    public static void parserCRUD(String sql){
        CCJSqlParserManager pm = new CCJSqlParserManager();

        Statement statement = null;
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
                if(isUnique(cols)){
                    results[0]="10";
                    results[1]=create.getTable().getName(); //表名
                    results[2]=colDef.toString();   // 表结构
                }else{
                    System.out.println("非标准SQL");
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
        String sql[] = StringToSql(str);
        if(sql[0].toUpperCase().equals("CREATE") && sql[1].toUpperCase().equals("DATABASE")){
            results[0] = "11"; // database 11
            results[1] = sql[2];  //数据库名称
        }else{
            parserCRUD(str);
        }
        return results;
    }
}