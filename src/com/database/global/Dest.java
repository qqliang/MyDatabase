package com.database.global;
/**
 * 操作目标
 * 表、数据库、索引、视图
 * @author zoe
 *
 */
public class Dest {
	public final static boolean SUCCESS=true;
	public final static boolean ERROR=false;
	public final static short OPENED=1;     //数据库打开状态
	public final static short CLOSED=2;     //数据库关闭状态
	public final static short TABLE = 10;   //创建表返回关键字状态码
	public final static short DATABASE = 11; //创建数据库返回关键字状态码
	public final static short INDEX = 12;    
	public final static short VIEW = 13;
	public final static short SELECT=21;     //select语句关键字状态码
	public final static short INSERT=22;     //insert语句关键字状态码
	public final static short DELETE=23;     //delete语句关键字状态码
	
	
	//定义数据类型状态
	
	public final static short BYTE=1;
	public final static short SHORT=2;
	public final static short INT=3;
	public final static short LONG=4;
	public final static short FLOAT=5;
	public final static short DOUBLE=6;
	public final static short CHAR=7;
	public final static short BOOLEAN=8;
	
	//定义基本数据类型长度
	
	public final static short BYTE_LENGTH=1;
	public final static short SHORT_LENGTH=2;
	public final static short INT_LENGTH=4;
	public final static short LONG_LENGTH=8;
	public final static short FLOAT_LENGTH=4;
	public final static short DOUBLE_LENGTH=8;
	public final static short CHAR_LENGTH=2;
	public final static short BOOLEAN_LENGTH=1;
	
	
	
	
}
