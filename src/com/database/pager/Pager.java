package com.db.pager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Pager {
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
			file = new File(dir, fileName);
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
	
	/**
	 * 为表添加元数据
	 * @param path 写入的目录路径
	 * @param tableName	表名
	 * @param row	写入的每一行的行数据
	 * @return
	 */
	public boolean writeMeta(String path, String tableName, String row){
		String dir = "";
		String fileName = tableName+"-meta";
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
	public boolean writeMeta(String path, String tableName, List<String> rows){
		String dir = "";
		String fileName = tableName + "-meta";
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
	 * @param row	写入的每一行的行数据
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
	 * 读取表元数据
	 * @param path 写入的目录路径
	 * @param tableName	表名
	 * @param row	写入的每一行的行数据
	 * @return 表的所有行
	 */
	public List<String> readMeata(String path, String tableName){
		String dir = "";
		String fileName = tableName + "-meta";
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
}
