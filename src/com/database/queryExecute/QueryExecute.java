package com.database.queryExecute;
import com.database.global.*;

public class QueryExecute {
	
	

	public boolean queryDo(int stat,String param){
		String str=param;
		int state=stat;
		if(state==11){
			//���ô������ݿ���䣬���ش����ɹ���ʧ��״̬
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
				//�����ݿ�
				switch(state){
				
				case 10:
					
				}
				
			}
			
		}
		
		
			
			
		
		return true;
	}
	
	
	
	
	

}
