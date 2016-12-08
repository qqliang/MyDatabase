package com.database.queryExecute;
import java.io.File;
import java.io.IOException;
import java.util.List;

import com.database.global.*;
import com.database.myBplusTree.BplusTree;
import com.database.pager.Pager;
import com.database.pager.Table;

public class Execute {
	private static String path = "D:/db";//根目录
	private Database db;

	//构造函数
	public Execute (Database db){
		this.db = db;
	}
	public Execute (){}

	//查询执行
	public void queryDo(String[] param){
		/**
		 * param[0]: 执行操作；
		 * 		11 —— 创建数据库
		 * 		10 —— 创建表
		 * 		22 —— 插入数据 insert
		 * 		21 —— 查询数据 select
		 * 		23 —— 删除数据 delete
		 * 	paramp[1]: 与操作相对应的参数
		 */
		int stat=Integer.parseInt(param[0]);
		
		switch(stat){
		case 11:
			/**
			 * 创建数据库
			 * 	1、在指定路径下面创建数据库文件(文件名与数据库名一致)
			 * 	2、在page1中填写数据库header
			 * 	3、打开该数据库
			 */
			try{
				File file = new File(path + "/" +param[1]);
				if (!file.exists()) {
                    file.createNewFile();
					//调用pager对象创建page1
					Pager pager = db.getPager();
					//打开该数据库
					if(db.getStat() == 0){
						db.setDBName(param[1]);
						db.setStat(1);
					}
					System.out.println("数据库创建成功！");
				}else{
					System.out.println("创建的数据库名字以存在！");
				}
			}catch(Exception e){
				System.out.println("创建数据库出错!");
				e.printStackTrace();
			}
			break;
		case 10:
			/**
			 * 创建表
			 * 1、创建该表的B+树
			 * 2、向page1中填写表与B+树的对应关系
			 */
			if(db.getStat() != 1){
				System.out.println("未打开数据库，请先打开数据库!");
			}else{
				BplusTree tree = new BplusTree(3,db);
				db.addTableTree(param[1], tree);
			}
			break;
		case 22:
			/**
			 * insert操作
			 * 1、提取参数中的value值
			 * 2、从db中找到该表的B+树
			 * 3、调用插入函数
			 */

			if(db.getStat()!=1){
				System.out.println("未打开数据库，请先打开数据库!");
			}else{
				String value = param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')'));
				String tableName = param[1];
				BplusTree tree = db.getTableTreeByName(tableName);
				tree.Insert(value);
			}
			break;
		case 21:
			/**
			 * select操作
			 * 1、获取要查询的字段、表名、条件
			 * 2、从db中获取该表的B+树
			 * 3、
			 */
			if(db.getStat() != 1){
				System.out.println("未打开数据库，请先打开数据库!");
			}else{
				String selectParam = param[1];		//查询字段
				String value[] =param[3].split("=");//查询条件
				String tableName = param[2];		//查询表名
				BplusTree tree = db.getTableTreeByName(tableName);

				List<String> results = null;
				if(value[0].equals("id"))
				{
					String result = tree.SelectByKey(value[1]);
					results.add(result);
				}else{
					results = tree.SelectByOther(value[0],value[1]);
				}
			}
			break;
		case 23:
			//delete操作
			if(db.getStat() != 1){
				System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
			}else{
				String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
				int id=Integer.parseInt(str[0]);
//				String result = db.getTree().Remove(id);
			}
		}
	}

}
