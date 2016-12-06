package com.database.parse;

/**
 * Created by houhao on 2016/11/30.
 */

public class CRUD {

    static String results[] = new String[5];  // 返回 create、insert、select 、delete语句的 关键参数信息

    public static String[] StringToSql(String str) {
        String sql[] = str.split(" ");
        return sql;
    }


    /**
     * 建库、建表
     * @param str
     * @return
     */
    public static String[] parser(String str) {
        String sql[] = StringToSql(str);
        switch (sql[0].toUpperCase()) {
            case "CREATE":
                if (sql[1].toUpperCase().equals("DATABASE")) {
                    String databaseName = sql[2]; // 数据库名
                    results[0] = "11"; // database 11
                    results[1] = databaseName;
                } else if (sql[1].toUpperCase().equals("TABLE")) {
                    String tableName = sql[2]; // 表名
                    String tableStruct = str.substring(str.indexOf('(') + 1, str.indexOf(')')); // 表结构
                    results[0] = "10"; // table 10
                    results[1] = tableName;
                    results[2] = tableStruct;
                } else {
                    results[0] = "0";
                    results[1] = "不是标准的SQL语句";
                }
                break;
            case "SELECT":
                if (sql[2].toUpperCase().equals("FROM")) {
                    String selectParam = str.substring(6, str.indexOf("from")); // 要查询的字段，以','
                    String tableName = "";
                    if (str.contains("where")) {

                        if (sql[4].toUpperCase().equals("WHERE")) {
                            tableName = str.substring(str.indexOf("from") + 5, str.indexOf("where")); // 表名
                            String whereParam = str.substring(str.indexOf("where") + 6, str.length()); // 查询条件
                            results[0] = "21";
                            results[1] = selectParam;
                            results[2] = tableName;
                            results[3] = whereParam;
                        } else {
                            results[0] = "0";
                            results[1] = "不是标准的SQL语句";
                        }
                    } else {
                        tableName = str.substring(str.indexOf("from") + 5, str.length()); // 表名
                        results[0] = "21";
                        results[1] = selectParam;
                        results[2] = tableName;
                    }
                } else {
                    results[0] = "0";
                    results[1] = "不是标准的SQL语句";
                }
                break;
            case "INSERT":
                if (sql[1].toUpperCase().equals("INTO")) {
                    if (sql[3].toUpperCase().equals("VALUES")) {
                        String tableName = sql[2];
                        String valueParam = sql[4]; // 插入内容
                        results[0] = "22";
                        results[1] = tableName;
                        results[2] = valueParam;
                    } else {
                        results[0] = "0";
                        results[1] = "不是标准的SQL语句";
                    }
                } else {
                    results[0] = "0";
                    results[1] = "不是标准的SQL语句";
                }
                break;
            case "DELETE":
                if (sql[1].toUpperCase().equals("FROM")) {
                    if (sql[3].toUpperCase().equals("WHERE")) {
                        String tableName = sql[2]; // 表名
                        String whereParam = str.substring(str.indexOf("where") + 6, str.length()); // 删除条件
                        results[0] = "23";
                        results[1] = tableName;
                        results[2] = whereParam;
                    } else {
                        results[0] = "0";
                        results[1] = "不是标准的SQL语句";
                    }
                } else {
                    results[0] = "0";
                    results[1] = "不是标准的SQL语句";
                }
                break;
            default:
                results[0] = "0";
                results[1] = "不是标准的SQL语句";
        }

        return results;

    }
}