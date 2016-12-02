package com.database.queryExecute;
import java.io.File;
import java.io.IOException;

import com.database.global.*;
import com.database.myBplusTree.BplusTree;

public class Execute {
	static File file = null;
    static String path = "d:";
    static String database = null;
    static String table = null;
	private Databases db ;

	public Execute(Databases db){
		this.db = db;
	}
	public Execute(){}

	public void queryDo(String[] param){
		int stat=Integer.parseInt(param[0]);

		switch(stat){
			
		case 11:          //创建数据库
			try{
				file = new File(path + "/" + database);
				if (!file.exists()) {
                    file.mkdir();
					db = new Databases(param[1]);
				}else{
					System.out.println("创建的数据库名字以存在！");
					//return false;
				}
				
				}catch(Exception e){
					System.out.println("create database error!");
					e.printStackTrace();
					
				}
			break;
		case 10:            //创建表
			if(db.stat!=1){
				System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
				//return false;
			}else{
				table = param[1] + ".txt";
                file = new File(db.getFile(), table);
                if (!file.exists()) {
                    try {
                        file.createNewFile();
						db.tree = new BplusTree(3,db);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }else{
					System.out.println("数据库已存在");
				}
			}
			break;
			
			case 22:    //insert操作
				
				if(db.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					String value=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')'));
					int id=Integer.parseInt(value.split(",")[0]);
			        db.tree.Insert(id,value);      //插入数据，id为主键，value为数据，例如  （1，“”）
				}
				break;
				
			case 21:    //select操作
				
				if(db.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
					int id=Integer.parseInt(str[0]);
					String result = db.tree.Search(id);             //select 操作，id为主键
					System.out.println("查询结果!");
					System.out.println(result);
				}
				
			 break;
			 
			 
			case 23:       //delete操作
				
				if(db.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
					int id=Integer.parseInt(str[0]);
					String result = db.tree.Remove(id);
					System.out.println(result);
				}
		
		
		}
		//return true;
	}

}
