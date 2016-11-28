package com.database.parse;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

public class testParser{

    public static void main(String[] args){
        CCJSqlParserManager pm = new CCJSqlParserManager();
        String sql = "SELECT * FROM MY_TABLE1, MY_TABLE2, (SELECT * FROM MY_TABLE3) LEFT OUTER JOIN MY_TABLE4 "+
                " WHERE ID = (SELECT MAX(ID) FROM MY_TABLE5) AND ID2 IN (SELECT * FROM MY_TABLE6)" ;
        Statement statement = null;
        try{
             statement = pm.parse(new StringReader(sql));
        }catch (Exception e){
            e.printStackTrace();
        }
        /*
        now you should use a class that implements StatementVisitor to decide what to do
        based on the kind of the statement, that is SELECT or INSERT etc. but here we are only
        interested in SELECTS
        */
        if (statement instanceof Select) {
            Select selectStatement = (Select) statement;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List tableList = tablesNamesFinder.getTableList(selectStatement);
            for (Iterator iter = tableList.iterator(); iter.hasNext();) {
                String table = (String )iter.next();
                System.out.println("表："+table);
            }
        }
    }
}