package com.database.queryExecute;
import com.database.parse.*;
import com.database.myBplusTree.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

import com.database.global.*;
import com.database.myBplusTree.BplusTree;
public class Execute {
	static File file = null;
    static String path = "d:";
    static String database = null;
    static String table = null;
	public void queryDo(String[] param){
		int stat=Integer.parseInt(param[0]);
		
		switch(stat){
			
		case 11:          //�������ݿ�
			try{
				file = new File(path + "/" + database);
				if (!file.exists()) {
                    file.mkdir();
                    Databases.name=param[1];
                    Databases.stat=1;
					}
				else{
					System.out.println("���������ݿ������Դ��ڣ�");
					//return false;
				}
				
				}catch(Exception e){
					System.out.println("create database error!");
					e.printStackTrace();
					
				}
			break;
		case 10:            //������
			if(Databases.stat!=1){
				System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
				//return false;
			}else{
				table = param[1] + ".txt";
                file = new File(path, table);
                if (!file.exists()) {
                    try {
                        file.createNewFile();
                        
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                   // return true;
                }
			}
			break;
			
			case 22:    //insert����
				
				if(Databases.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					
					String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
					int id=Integer.parseInt(str[0]);
					//String value=str[1];
			        Insert(id,str);      //�������ݣ�idΪ������strΪ���ݣ�����  ��1��������
					
				}
				
				
				break;
				
			case 21:    //select����
				
				if(Databases.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
					int id=Integer.parseInt(str[0]);
					String result=Select(id);                //select ������idΪ����
					                            
					
					
				}
				
			 break;
			 
			 
			case 23:       //delete����
				
				if(Databases.stat!=1){
					System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
					//return false;
				}else{
					String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
					int id=Integer.parseInt(str[0]);
					String result=Remove(id);
				}
		
		
		}
		//return true;
	}

}
