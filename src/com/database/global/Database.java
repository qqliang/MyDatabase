package com.database.global;

import com.database.myBplusTree.BplusTree;
import com.database.parse.CRUD;
import com.database.queryExecute.Execute;

import java.io.File;

public class Database {

	private String dbName; //数据库名称
	private int stat; //数据库状态，打开为1，关闭为0
	private BplusTree tree; //数据库B树

	private static String path = "D:/db";//根目录

	private File dbFile = null;//数据库文件
	private String tableName = null;//表名称

	//构造函数
	public Database(String dbName){
		tree = new BplusTree(3,this);
		this.dbName = dbName;
		setStat(1);
	}
	public Database(){}

	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public void setDBName(String dbName) { this.dbName = dbName; }
	public String getDBName() {
		return dbName;
	}
	public BplusTree getTree() {
		return tree;
	}
	public void setTree(BplusTree tree) {
		this.tree = tree;
	}

	public int getStat() {
		return stat;
	}
	public void setStat(int stat) {
		this.stat = stat;
	}
	public String getDBFile(){
		return this.dbFile.getAbsolutePath();
	}
	public void setDBFile(File dbFile){
		this.dbFile = dbFile;
	}

	/**
	 * 根据数据库名称，打开数据库
	 * @param dbName
	 * @return 打开成功为1，否则为0
	 */
	public int openDB(String dbName){
		dbFile = new File(path + "/" + dbName);
		if (!dbFile.exists()) {
			return 0;//打开不成功
		}else{
			setStat(1);
			return 1;//打开成功
		}
	}

	/**
	 * sql语句的执行
	 * @param sql
	 */
	public void exeSQL(String sql){
		String result[] = CRUD.parser(sql);
		if(result[0] != "0" )
		{
			Execute execute = new Execute(this);
			execute.queryDo(result);
		}
	}
}
