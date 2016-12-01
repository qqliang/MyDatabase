package com.database.global;
/**
 * 操作类型
 * @author zoe
 *
 */
public class Operation {
	public final static short CREATE = 0;
	public final static short OPENED=1;     //数据库打开状态
	public final static short CLOSED=2;     //数据库关闭状态
	public final static short TABLE = 10;   //创建表返回关键字状态码
	public final static short DATABASE = 11; //创建数据库0返回关键字状态码
	public final static short SELECT=21;     //select语句关键字状态码
	public final static short INSERT=22;     //insert语句关键字状态码
	public final static short DELETE=23;     //delete语句关键字状态码
}
