package com.database.global;

import com.database.myBplusTree.BplusTree;
import com.database.parse.CRUD;
import com.database.queryExecute.Execute;

import java.io.File;

public class Databases {
	
	public String name;
	public int stat;
	public BplusTree tree;
	static String path = "d:";
	static File file = null;

	public String getOpenFile() {
		return openFile;
	}

	public void setOpenFile(String openFile) {
		this.openFile = openFile;
	}

	public String openFile = null;

	public Databases(String name){
		tree = new BplusTree(3,this);
		this.name = name;
		setStat(1);
	}
	public Databases(){}

	public String getName() {
		return name;
	}
	public int getStat() {
		return stat;
	}
	public void setStat(int stat) {
		this.stat = stat;
	}
	public String getFile(){return this.file.getAbsolutePath();}

	public int openDB(String filename){
		file = new File(path + "/" + filename);
		if (!file.exists()) {
			return 0;
		}else{
			setStat(1);
			return 1;
		}
	}
	public void exeSQL(String sql){
		String result[] = CRUD.parser(sql);
		Execute execute = new Execute(this);
		execute.queryDo(result);
	}
}
