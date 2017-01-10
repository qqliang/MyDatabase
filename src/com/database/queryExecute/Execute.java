package com.database.queryExecute;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.database.global.*;
import com.database.myBplusTree.BplusTree;
import com.database.pager.Column;
import com.database.pager.Page;
import com.database.pager.Pager;
import com.database.pager.TableSchema;

public class Execute {
	private static String path = "D:/db";//根目录
	private Database db;
	private Pager pager;

	//构造函数
	public Execute (Database db,Pager pager){
		this.db = db;
		this.pager = pager;
	}

	//查询执行
	public void queryDo(String[] param, String sql){
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
					db.setDBFile(file);
					/* 调用pager对象创建page1 */
					Page page1 = pager.aquireNewPage();
					page1.setTableCount(0);			//设置目前表的计数为0
					pager.updateHeader(page1);
					db.setPage1(page1);
					//打开该数据库
					if(db.getStat() == 0){
						db.setDBName(param[1]);		//设置数据库名称
						db.setStat(1);				//设置数据库状态
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
				String schemaSQL = param[2].substring(param[2].indexOf('[')+1,param[2].indexOf(']'));
				TableSchema schema = TableSchema.buildTableSchema(schemaSQL);
				BplusTree tree = new BplusTree(3,db,schema);
				db.addTableTree(param[1], sql, tree);
				System.out.println("创建表"+param[1]+"成功");
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
				if(tree != null)
				{
					tree.Insert(value);
				}else{
					System.out.println("数据库中没有该表！");
				}
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
				/* 查询字段 */
				String selectParam = param[1];
				List<String> selectStr = new ArrayList<>();
				if( selectParam == "*" ){
//					selectStr = "all";
				}

				/* 查询表名 */
				String tableName = param[2];
				BplusTree tree = db.getTableTreeByName(tableName);

				/* 查询条件 */
				String[] value;
				if( param.length == 4 ){
					value = param[3].split("=");
				}else {
					value = null;
				}

				/* 查询结果 */
				/**
				 * 1、没有查询条件
				 * 2、有查询条件
				 * 		2.1 查询条件为主键
				 * 		2.2 查询条件为其他
				 */
				List<String> results = new ArrayList<>();
				if(value == null){
					results = tree.SelectAll();
				}else{
					if(value[0].equals("id"))
					{
						String result = tree.SelectByKey(Integer.parseInt(value[1]));
						results.add(result);
					}else{
						results = tree.SelectByOther(value[0],value[1]);
					}
				}

				/* 输出结果 */
				if (results.size()==0){
					System.out.println("未找到结果！");
				}else{
					/* 输出表结构 */
					TableSchema schema = tree.getHead().schema;
					List<Column> columns = schema.getColumns();
					for(int i=0;i<columns.size();i++){
						Column column = columns.get(i);
						System.out.print(column.getName()+ "	");
					}
					System.out.println();

					/* 按行输出数据 */
					for ( int i=0 ;i < results.size(); i++ ){
						String[] result = results.get(i).split(",");
						for(int j=0;j<result.length;j++) {
							System.out.print(result[j]+"	");
						}
						System.out.println();
					}
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
