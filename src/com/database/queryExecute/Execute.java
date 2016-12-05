package com.database.queryExecute;
import java.io.File;
import java.io.IOException;
import com.database.global.*;
import com.database.myBplusTree.BplusTree;

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
			//�������ݿ�
			try{
				File file = new File(path + "/" +param[1]);
				if (!file.exists()) {
                    file.mkdir();
					//���û�д򿪵����ݿ⣬�򽫴򿪸����ݿ�
					if(db.getStat() == 0){
						db.setDBName(param[1]);
						db.setStat(1);
					}
					db.setDBFile(file);
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
			//������
			if(db.getStat() != 1){
				System.out.println("δ�����ݿ⣬���ȴ����ݿ�!");
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
                    System.out.println(param[1]+"�����ɹ�");
                }else{
					System.out.println("�ñ��Ѵ��ڣ�");
				}
			}
			break;
		case 22:
			//insert����
			if(db.getStat()!=1){
				System.out.println("δ�����ݿ⣬���ȴ����ݿ�!");
			}else{
				String value = param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')'));
				int id=Integer.parseInt(value.split(",")[0]);
				db.setTableName(param[1]);
				db.getTree().Insert(id,value);      //�������ݣ�idΪ������strΪ���ݣ�����  ��1��������
			}
			break;
		case 21:
			//select����
			if(db.getStat() != 1){
				System.out.println("δ�����ݿ⣬���ȴ����ݿ�!");
			}else{
				String str[]=param[2].substring(param[2].indexOf('(')+1,param[2].indexOf(')')).split(",");
				int id=Integer.parseInt(str[0]);
				String result = db.getTree().Search(id);                //select ������idΪ����
			}
			break;
		case 23:
			//delete����
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
