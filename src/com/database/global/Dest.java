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
}
