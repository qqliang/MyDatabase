package com.database.global;

import com.database.myBplusTree.BplusTree;
import com.database.parse.CRUD;
import com.database.queryExecute.Execute;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Database {

	private String dbName; 					//数据库名称
	private int stat; 						//数据库状态，打开为1，关闭为0
	private BplusTree tree; 				//数据库B树
	private byte[] header ;

	private static String path = "D:/db";	//根目录

	private File dbFile = null;				//数据库文件
	private String tableFile = null;		//表文件

	public Database(String parentPath, String dbName)
	{
		tree = new BplusTree(3,this);

		this.dbName = dbName;
		File dir = new File(parentPath);
		File file = null;

		/**
		 * 如果给定目录不存在，将数据库创建在当前目录下
		 */
		if (!dir.isDirectory()) {
			dir = null;
		}
		if (dir != null) {
			file = new File(dir, dbName);
		} else {
			file = new File(dbName);
		}

		this.dbFile = file;
		setStat(1);
	}
	/**
	 * 读取数据库头信息
	 * @return
	 */
	public byte[] getHeader()
	{
		if(this.header != null){
			return header;
		}

		if(this.dbFile == null)
			return null;
		FileInputStream fis = null;

		try{

			fis = new FileInputStream(this.dbFile);
			this.header = new byte[100];
			fis.read(header,0,100);

		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}finally {
			try{
				if(fis != null) fis.close();
			}catch (IOException e){
				e.printStackTrace();
			}
		}

		return header;
	}
	public void setHeader(byte[] header){
		this.header = header;
	}

	//构造函数
	public Database(String dbName){
		tree = new BplusTree(3,this);
		this.dbName = dbName;
		setStat(1);
	}
	public Database(){}

	public String getTableFile() {
		return tableFile;
	}
	public void setTableFile(String tableFile) {
		this.tableFile = tableFile;
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
	public File getDBFile(){
		return this.dbFile;
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
		Execute execute = new Execute(this);
		execute.queryDo(result);
	}
}
