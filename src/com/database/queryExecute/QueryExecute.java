package com.database.queryExecute;
import com.database.global.*;

public class QueryExecute {
	
	

	public boolean queryDo(int stat,String param){
		String str=param;
		int state=stat;
		if(state==11){
			//调用创建数据库语句，返回创建成功和失败状态
			boolean flag=false;
			//int flag=;
			if(flag){
				Databases.name=param;
				Databases.stat=1;
				return true;
			}else{
				System.out.println("create database error!");
				return false;
			}
		}else{
			UseDatabases.setDatabase("");
			if(Databases.stat!=1){
				System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
				return false;
			}
			else{
				//打开数据库
				switch(state){
				
				case 10:
					
				}
				
			}
			
		}
		
		
			
			
		
		return true;
	}
	
	
	
	
	

}
