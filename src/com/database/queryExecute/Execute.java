package com.database.queryExecute;
import java.io.File;
import java.io.IOException;
import java.util.List;

import com.database.global.*;
import com.database.myBplusTree.BplusTree;
import com.database.pager.Pager;
import com.database.pager.Table;

public class Execute {
	private static String path = "D:/db";//��Ŀ¼
	private Database db;

	//���캯��
	public Execute (Database db){
		this.db = db;
	}
	public Execute (){}

	//��ѯִ��
	public void queryDo(String[] param){
		/**
		 * param[0]: ִ�в�����
		 * 		11 ���� �������ݿ�
		 * 		10 ���� ������
		 * 		22 ���� �������� insert
		 * 		21 ���� ��ѯ���� select
		 * 		23 ���� ɾ������ delete
		 * 	paramp[1]: ��������Ӧ�Ĳ���
		 */
		int stat=Integer.parseInt(param[0]);
		
		switch(stat){
		case 11:
			/**
			 * �������ݿ�
			 * 	1����ָ��·�����洴�����ݿ��ļ�(�ļ��������ݿ���һ��)
			 * 	2����page1����д���ݿ�header
			 * 	3���򿪸����ݿ�
			 */
			try{
				File file = new File(path + "/" +param[1]);
				if (!file.exists()) {
                    file.createNewFile();
					//����pager���󴴽�page1
					Pager pager = db.getPager();
					//�򿪸����ݿ�
					if(db.getStat() == 0){
						db.setDBName(param[1]);
						db.setStat(1);
					}
					System.out.println("���ݿⴴ���ɹ���");
				}else{
					System.out.println("���������ݿ������Դ��ڣ�");
				}
			}catch(Exception e){
				System.out.println("�������ݿ����!");
				e.printStackTrace();
			}
			break;
		case 10:
			/**
			 * ������
			 * 1�������ñ��B+��
			 * 2����page1����д����B+���Ķ�Ӧ��ϵ
			 */
			if(db.getStat() != 1){
				System.out.println("δ�����ݿ⣬���ȴ����ݿ�!");
			}else{
				BplusTree tree = new BplusTree(3,db);
				db.addTableTree(param[1], tree);
			}
			break;
		case 22:
			/**
			 * insert����
			 * 1����ȡ�����е�valueֵ
			 * 2����db���ҵ��ñ��B+��
			 * 3�����ò��뺯��
			 */

			if(db.getStat()!=1){
				System.out.println("δ�����ݿ⣬���ȴ����ݿ�!");
			}else{
				String value = param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')'));
				String tableName = param[1];
				BplusTree tree = db.getTableTreeByName(tableName);
				tree.Insert(value);
			}
			break;
		case 21:
			/**
			 * select����
			 * 1����ȡҪ��ѯ���ֶΡ�����������
			 * 2����db�л�ȡ�ñ��B+��
			 * 3��
			 */
			if(db.getStat() != 1){
				System.out.println("δ�����ݿ⣬���ȴ����ݿ�!");
			}else{
				String selectParam = param[1];		//��ѯ�ֶ�
				String value[] =param[3].split("=");//��ѯ����
				String tableName = param[2];		//��ѯ����
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
			//delete����
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
