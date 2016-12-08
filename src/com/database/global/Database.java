package com.database.global;

import com.database.myBplusTree.BplusTree;
import com.database.pager.Pager;
import com.database.pager.Table;
import com.database.parse.parser;
import com.database.queryExecute.Execute;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {

	private static String path = "D:/db";				//根目录
	private String dbName; 								//数据库名称
	private File dbFile;								//数据库文件
	private int stat; 									//数据库状态，打开为1，关闭为0
	private Pager pager; 								//pager对象

	private Map<String,BplusTree> tableTreeMap;			//表与B+树映射列表

	//构造函数
	public Database(){
		this.pager = new Pager(this);
	}

	/** 设置和获取数据库名称 */
	public void setDBName(String dbName) {
		this.dbName = dbName;
	}
	public String getDBName() {
		return dbName;
	}

	/** 设置和获取数据库状态 */
	public int getStat() {
		return stat;
	}
	public void setStat(int stat) {
		this.stat = stat;
	}

	/** 设置和获取数据库文件 */
	public String getDBFile() {
		return dbFile.getAbsolutePath();
	}
	public void setDBFile(File dbFile) {
		this.dbFile = dbFile;
	}

	/** 设置和获取数据库名字 */
	public Pager getPager() {
		return pager;
	}
	public void setPager(Pager pager) {
		this.pager = pager;
	}

	/** 向表树映射中添加映射关系 */
	public void addTableTree(String tableName, BplusTree tree){
		tableTreeMap.put(tableName, tree);
	}
	/** 根据表名获取B+树 */
	public BplusTree getTableTreeByName (String tableName){
		return (BplusTree)tableTreeMap.get(tableName);
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
		String result[] = parser.parser(sql);
		if(result[0] != "0" )
		{
			Execute execute = new Execute(this);
			execute.queryDo(result);
		}
	}
}
