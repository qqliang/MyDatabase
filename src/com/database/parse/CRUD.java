package com.database.parse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by houhao on 2016/11/30.
 */

public class CRUD {

    static String str = null;
    static File file = null;
    static String path = "d:";
    static String database = null;
    static String table = null;
    static String results[] = new String[5];  // 返回 create、insert、select 语句的 关键参数信息

    public static String[] StringToSql(String str) {
        String sql[] = str.split(" ");
        return sql;
    }


    /**
     * 建库、建表
     * @param str
     * @return
     */
    public static String[] create(String str) {
        String sql[] = StringToSql(str);
        if (sql[0].equals("create") || sql[0].equals("CREATE")) {
            if (sql[1].equals("database") || sql[1].equals("DATABASE")) {
                database = sql[2];
                file = new File(path + "/" + database);
                if (!file.exists()) {
                    file.mkdir();
                    results[0] = "11";	// database 11
                    results[1] = database; //返回 数据库名
                }else{
                    results[0] = "0";
                    results[1] = "数据库已存在";
                }
            }else if (sql[1].equals("table") || sql[1].equals("TABLE")) {
                table = sql[2] + ".txt";
                file = new File(path, table);
                if (!file.exists()) {
                    try {
                        file.createNewFile();
                        results[0]="10";	// table 10
                        results[1] = sql[2];	//表名
                        results[2]=str.substring(str.indexOf('(')+1,str.indexOf(')')); //表结构
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }else{
                    results[0] = "false";
                    results[1] = "数据表已存在";
                }
            }
        }else {
            results[0] = "false";
            results[1] = "关键字错误";
        }
        return results;

    }

    public static String[] insert(String str) {
        String sql[] = StringToSql(str);
        if(sql[0].equals("insert") || sql[0].equals("INSERT")){
            String tableName=sql[2];
            String values=sql[4];
            results[0]="22";
            results[1]=tableName;
            results[2]=values;
        }else{
            results[0] = "false";
            results[1] = "不是标准的SQL语句";
        }
        return results;
    }

    public static String[] select(String str){
        String sql[]=StringToSql(str);
        if(sql[0].equals("select") || sql[1].equals("SELECT")){
            results[0]="21";
            results[1]=str.substring(6,str.indexOf("from")); //要查询的字段，以',' 分开
            if(str.contains("where")){
                results[2]=str.substring(str.indexOf("from")+5,str.indexOf("where")); //表名
            }else{
                results[2]=str.substring(str.indexOf("from")+5,str.length()); //表名
            }

        }else{
            results[0] = "false";
            results[1] = "不是标准的SQL语句";
        }
        return results;
    }

    public static void main(String[] args) {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        try {
            str = in.readLine();
            System.out.println(str);
        } catch (IOException e) {

            e.printStackTrace();
        }

        //String result[]=create(str);
        //String result[]=insert(str);
        String result[]=select(str);
        for(int i=0;i<result.length;i++){
            System.out.println(result[i]);
        }


    }

}