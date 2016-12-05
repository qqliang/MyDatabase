package com.database.pager;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.database.global.*;

import javax.xml.crypto.Data;

public class Pager {
	Database database;
	File journal;
	PCache pCache;
	Page[] pages;
	int pageNum;
	public Pager(Database database) {
		this.database = database;
//		this.journal = new File(this.database.getDBName()+"-journal");
		this.pCache = null;
		this.pageNum = 2000;
		initPages();
	}

	public int getPageNum() {
		return pageNum;
	}

	public void setPageNum(int pageNum) {
		this.pageNum = pageNum;
	}

	public void initPages(){
		this.pages = new Page[this.pageNum];
		for(int i =0 ;i <this.pages.length; i++){
			Page page = new Page();
			page.setData(new byte[SpaceAllocation.PAGE_SIZE]);
			page.setPgno(i);
			page.setOverflow(null);
			page.setSectorSize(SpaceAllocation.SECTOR_SIZE);
			page.setSize(SpaceAllocation.PAGE_SIZE);
			pages[i] = page;
		}
	}
	public void writeRootPage(Page page){
		File dbFile = new File(database.getDBFile());
		byte[] rootPageData = new byte[SpaceAllocation.PAGE_SIZE];
		FileOutputStream fos = null;
		try{
			fos = new FileOutputStream(dbFile);
			fos.write(rootPageData,0,SpaceAllocation.PAGE_SIZE);
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	/**
	 * 像指定页面中写入数据
	 * @param pgno 要写入数据的页号
	 * @param data	要写入的数据，可以通过Record的getBytes方法可以简单得到
	 */
	public void writeData(int pgno, byte[] data){

		File dbFile = new File(database.getDBFile());
		Page page = this.pages[pgno];
		page.fillData(data);
		FileOutputStream fos = null;
		try{
			fos = new FileOutputStream(dbFile);
			fos.write(page.getData());
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e){
			e.printStackTrace();
		}finally {
			try{
				if(fos!=null)fos.close();
			}catch (IOException e){
				e.printStackTrace();
			}
		}
	}
	/**
	 * 读取指定页面中的数据
	 * @param pgno 要写入数据的页号
	 * @return 指定页面的数据
	 */
	public byte[] readData(int pgno){
		if(pgno <= 0 )
			return null;

		return this.pages[pgno].getData();
	}
	/**
	 * 读取指定页面中的数据返回记录
	 * @param pgno 要写入数据的页号
	 * @return 指定页面的记录
	 */
	public List<String> readRecord(int pgno){
		if(pgno <= 0 )
			return null;
		List<String> result = new ArrayList<String>();
		Page page = this.pages[pgno];
		int offset = page.getOffset();
		byte[] data = page.getData();
		int hdrSz = Utils.loadIntFromBytes(data, offset);
		int[] types = new int[hdrSz - SpaceAllocation.RECORD_HEADER];
		int dataSize = 0;
		for(int i =0; i < hdrSz - SpaceAllocation.RECORD_HEADER; i++){
			byte type = data[offset+SpaceAllocation.RECORD_HEADER + i];
			switch(type){
				case DataType.INTEGER:
					dataSize += 4;
					types[i] = DataType.INTEGER;
					break;
				case DataType.SMALL_INT:
					dataSize += 2;
					types[i] = DataType.SMALL_INT;
					break;
				case DataType.TINY_INT:
					dataSize += 1;
					types[i] = DataType.TINY_INT;
					break;
				case DataType.TEXT:
					dataSize += 50;
					types[i] = DataType.TEXT;
					break;
				case DataType.LONG:
					dataSize += 8;
					types[i] = DataType.LONG;
					break;
			}
		}
		for(int index = (offset+hdrSz); index < SpaceAllocation.PAGE_SIZE; ){
			List<String> cols = null;
			cols = loadColsFromBytes(data, types, index);
			String rec = cols.toString();
			result.add(colsToRow(cols));
			index += hdrSz + dataSize;
		}
		return result;
	}
	public String colsToRow(List<String> cols){
		StringBuilder row = new StringBuilder();
		for(String col : cols){
			row.append(col+",");
		}
		return row.substring(0,row.length()-1);
	}
	/**
	 *
	 * @param data		数据加载的地方
	 * @param types
	 * @param start		加载起始位置
	 * @return
	 */
	public List<String> loadColsFromBytes(byte[] data, int[] types, int start){
		List<String> cols = new ArrayList<String>();
		for(int i=0 ; i < types.length; i++){
			switch (types[i]){
				case DataType.INTEGER:
					cols.add(new Integer(Utils.loadIntFromBytes(data,start)).toString());
					start += 4;
					break;
				case DataType.SMALL_INT:
					cols.add(new Short(Utils.loadShortFromBytes(data,start)).toString());
					start += 2;
					break;
				case DataType.TINY_INT:
					cols.add(new Byte(data[start]).toString());
					start += 1;
					break;
				case DataType.TEXT:
					cols.add(Utils.loadStrFromBytes(data,start,50));
					start += 50;
					break;
				case DataType.LONG:
					cols.add(new Long(Utils.loadLongFromBytes(data,start)).toString());
					start += 8;
					break;
			}
		}

		return cols;
	}
	/**
	 * 为表添加数据
	 * @param path 写入的目录路径
	 * @param tableName	表名
	 * @param row	写入的每一行的行数据
	 * @return
	 */
	public boolean writeTable(String path, String tableName, String row){
		String dir = "";
		String fileName = tableName;
		boolean result = true;
		File file = null;
		
		
		if(path != null && path.length() > 0 ){
			dir = path;
			file = new File(dir, fileName+".txt");
		}else{
			file = new File(fileName);
		}
		BufferedWriter bw = null;
		FileWriter fw = null;
		
		try {
			fw = new FileWriter(file,true);
			bw = new BufferedWriter(fw);
			bw.write(row);
			bw.newLine();
			bw.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
			if(bw != null)
				try {
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if(fw != null)
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return result;
	}

	public boolean writeTable(String path, String tableName, List<String> rows){
		String dir = "";
		String fileName = tableName;
		boolean result = true;
		File file = null;
		
		
		if(path != null && path.length() > 0 ){
			dir = path;
			file = new File(dir, fileName);
		}else{
			file = new File(fileName);
		}
		BufferedWriter bw = null;
		FileWriter fw = null;
		
		try {
			
			fw = new FileWriter(file,true);
			bw = new BufferedWriter(fw);
			
			for(String row : rows){
				bw.write(row);
				bw.newLine();
			}
			bw.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
			if(bw != null)
				try {
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if(fw != null)
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return result;
	}
	/**
	 * 读取所有表数据 
	 * @param path 写入的目录路径
	 * @param tableName	表名
	 * @return 表的所有行
	 */
	public List<String> readTable(String path, String tableName){
		String dir = "";
		String fileName = tableName;
		boolean result = true;
		File file = null;
		List<String> rows = new ArrayList<String>();
		
		if(path != null && path.length() > 0 ){
			dir = path;
			file = new File(dir, fileName);
		}else{
			file = new File(fileName);
		}
		BufferedReader bw = null;
		FileReader fw = null;
		
		try {
			fw = new FileReader(file);
			bw = new BufferedReader(fw);
			String row = null;
			while((row = bw.readLine())!=null){
				rows.add(row);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
			if(bw != null)
				try {
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if(fw != null)
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return rows;
	}

	/**
	 *	加载数据库文件中的数据。
	 * @param dbPath 数据库路径名
	 */
	public void loadDB(String dbPath) {
		if(dbPath == null || dbPath.isEmpty())
			return;
		FileInputStream fis = null;
		try{
			fis = new FileInputStream(new File(dbPath));
			byte[] data = new byte[SpaceAllocation.PAGE_SIZE];
			int len = 0;
			int pgno = 0;
			/**
			 *	开始读取数据库：
			 * 第一次读取的根页面，后续为数据页面
			 */
			while((len = fis.read(data,0,data.length)) !=0 ){
				pgno ++ ;
				if(pgno < this.pageNum)
					this.pages[pgno].fillData(data);
				else{
					resizePages();
					this.pages[pgno].fillData(data);
				}
			}

		}catch (IOException e){
			e.printStackTrace();
		}finally {
			try{
				if(fis != null) fis.close();
			}catch (IOException e){
				e.printStackTrace();
			}
		}
	}
	private void resizePages()
	{
		int oldPageNum = this.pageNum;
		Page[] newPages = new Page[oldPageNum * 2];
		for(int i = 0; i< oldPageNum; i++){
			newPages[i] = this.pages[i];
		}
		for(int i = oldPageNum; i < newPages.length; i++){
			Page page = new Page();
			page.setData(new byte[SpaceAllocation.PAGE_SIZE]);
			page.setPgno(i);
			page.setOverflow(null);
			page.setSectorSize(SpaceAllocation.SECTOR_SIZE);
			page.setSize(SpaceAllocation.PAGE_SIZE);
			newPages[i] = page;
		}

		this.pages = newPages;
		this.pageNum = newPages.length;
	}
}
