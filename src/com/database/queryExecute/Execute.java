package com.database.queryExecute;
import java.io.File;
import java.io.IOException;
import com.database.global.*;
import com.database.myBplusTree.BplusTree;

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
			//创建数据库
			try{
				File file = new File(path + "/" +param[1]);
				if (!file.exists()) {
                    file.mkdir();
					//如果没有打开的数据库，则将打开该数据库
					if(db.getStat() == 0){
						db.setDBName(param[1]);
						db.setStat(1);
					}
					db.setDBFile(file);
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
			//创建表
			if(db.getStat() != 1){
				System.out.println("未打开数据库，请先打开数据库!");
			}else{
				String table = param[1] + ".txt";
                File file = new File(db.getDBFile(), table);
                if (!file.exists()) {
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    db.setTree(new BplusTree(3,db));
                    System.out.println(param[1]+"表创建成功");
                }else{
					System.out.println("该表已存在！");
				}
			}
			break;
		case 22:
			//insert操作
			if(db.getStat()!=1){
				System.out.println("未打开数据库，请先打开数据库!");
			}else{
				String value = param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')'));
				int id=Integer.parseInt(value.split(",")[0]);
				db.setTableName(param[1]);
				db.getTree().Insert(id,value);      //插入数据，id为主键，str为数据，例如  （1，“”）
			}
			break;
		case 21:
			//select操作
			if(db.getStat() != 1){
				System.out.println("未打开数据库，请先打开数据库!");
			}else{
				String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
				int id=Integer.parseInt(str[0]);
				String result = db.getTree().Search(id);                //select 操作，id为主键
			}
			break;
		case 23:
			//delete操作
			if(db.getStat() != 1){
				System.out.println("database you are performing is closed,please confirm your databse is opend firstly!");
			}else{
				String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
				int id=Integer.parseInt(str[0]);
				String result = db.getTree().Remove(id);
			}
		}
	}

}
