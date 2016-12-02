package com.database.queryExecute;
import com.database.parse.*;
import com.database.myBplusTree.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

import com.database.global.*;
import com.database.myBplusTree.BplusTree;
public class execute {
	static File file = null;
    static String path = "d:";
    static String database = null;
    static String table = null;
    public String getType(Object o){
    	
    	return o.getClass().getName().toString();
    }
	public void queryDo(String[] param){
		int stat=Integer.parseInt(param[0]);
		
		switch(stat){
			
		case 11:          //创建数据库
			try{
				file = new File(path + "/" + database);   
				if (!file.exists()) {        //判断数据库是否存在，根据创建判断
                    file.mkdir();
                    Databases.name=param[1];
                    Databases.stat=1;
					}
				else{
					System.out.println("创建的数据库名字以存在！");
					//return false;
				}
				
				}catch(Exception e){
					System.out.println("create database error!");
					e.printStackTrace();
					
				}
			break;
		case 10:            //创建表
			if(Databases.stat!=1){
				System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
				//return false;
			}else{
				String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
				String col="";
				int len=0;
				int type_len=0;
				for(int i=0;i<str.length;i++){
					String type=getType(str[i]);
					if(type.equals("java.lang.Short")){
						col+="2,";
						len+=1;
						type_len+=2;
					}else if(type.equals("java.lang.Integer")){
						col+="3,";
						len+=1;
						type_len+=4;
					}else if(type.equals("java.lang.Byte")){
						col+="1,";
						len+=1;
						type_len+=1;
					}else if(type.equals("java.lang.Long")){
						col+="4,";
						len+=1;
						type_len+=8;
					}else if (type.equals("java.lang.Float")){
						col+="5,";
						len+=1;
						type_len+=4;
					}else if(type.equals("java.lang.Double")){
						col+="6,";
						len+=1;
						type_len+=8;
					}else if(type.equals("java.lang.Char")){
						col+="7,";
						len+=1;
						type_len+=2;
					}else if(type.equals("java.lang.Boolean")){
						col+="8,";
					    len+=1;
					    type_len+=1;
					}else if(type.equals("java.lang.String")){
						col+="9,";
						len+=1;
						type_len+=20;
					}
					//执行创建表语句
				}
                if (!file.exists()) {       //判断表是否已存在，在创建的时候判断
                    try {
                        file.createNewFile();
                        
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                   // return true;
                }
			}
			break;
			
			case 22:    //insert操作
				
				if(Databases.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					
					String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
					int id=Integer.parseInt(str[0]);
					//String value=str[1];
			        //Insert(id,str);      //插入数据，id为主键，str为数据，例如  （1，“”）
					
				}
				
				
				break;
				
			case 21:    //select操作
				
				if(Databases.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
					int id=Integer.parseInt(str[0]);
					//String result=Select(id);                //select 操作，id为主键
					                            
					
					
				}
				
			 break;
			 
			 
			case 23:       //delete操作
				
				if(Databases.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
					int id=Integer.parseInt(str[0]);
					//String result=Remove(id);
				}
		
		
		}
		//return true;
	}

}
