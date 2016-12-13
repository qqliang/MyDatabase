package com.database.pager;

import java.io.*;
import java.util.*;
import com.database.global.*;


public class Pager {
	private Database database;
	private File journal;
	private PCache pCache;

	private int mxPgno;
	private int head;
	private int tableCount;

	public Pager(Database database) {
		this.database = database;
//		this.journal = new File(this.database.getDBName()+"-journal");
		this.pCache = new PCache();
	}

	public int getMxPgno() {
		return mxPgno;
	}

	public void setMxPgno(int mxPgno) {
		this.mxPgno = mxPgno;

	}

	public int getHead() {
		return head;
	}

	public void setHead(int head) {
		this.head = head;
	}

	public int getTableCount() {
		return tableCount;
	}

	public void setTableCount(int tableCount) {
		this.tableCount = tableCount;
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
	 * 像指定页面中写入数据，即刷新整个页面数据
	 * @param pgno 要写入数据的页号
	 * @param data	要写入的数据，entry中的byte[]可以通过Record的getBytes方法可以简单得到
	 */
	public void writeData(int pgno, List<Map.Entry<Integer, byte[]>> data){
		Page page = aquirePage(pgno);
		page.fillData(data);
		pCache.makeDirty(page);
//		pCache.printStatus();
	}

	public void commit(){

	}
	/**
	 * 读取指定页号对应的页面
	 * @param pgno	要获取的页面的页号
	 * @return
	 */
	public Page aquirePage(int pgno){
		Page page = this.pCache.fetch(pgno);

		if(page.getOffset() != SpaceAllocation.PAGE_SIZE)
			return page;
		page = loadPage(pgno, page);
		return page;
	}
	/**
	 * 读取指定页面中的数据
	 * @param pgno 要读取的页号
	 * @param rowid 要读取的rowid
	 * @return 指定页面的数据
	 */
	public Map.Entry<Integer,String>  readDataByRowid(int pgno, int rowid){
		if(pgno <= 0 )
			return null;
		Map.Entry<Integer,String> entry = null;			//返回的结果
		Page page = aquirePage(pgno);
		byte[] data = page.getData();
//		if(page.getPageType() == PageType.TABLE_LEAF){
			int offset = page.getOffset();
			if(offset == SpaceAllocation.PAGE_SIZE)
				return null;
			int szHdr = Utils.loadIntFromBytes(data, offset+4);
			int skip = szHdr;
			int[] types = new int[szHdr - SpaceAllocation.RECORD_HEADER];
			for(int col = 0 ; col< szHdr - SpaceAllocation.RECORD_HEADER; col++){
				switch (data[offset + SpaceAllocation.RECORD_HEADER + col]){
					case DataType.INTEGER:
						skip += 4;
						types[col] = DataType.INTEGER;
						break;
					case DataType.LONG:
						skip += 8;
						types[col] = DataType.LONG;
						break;
					case DataType.TINY_INT:
						skip += 1;
						types[col] = DataType.TINY_INT;
						break;
					case DataType.SMALL_INT:
						skip += 2;
						types[col] = DataType.SMALL_INT;
						break;
					case DataType.TEXT:
						skip += 50;
						types[col] = DataType.TEXT;
						break;
				}
			}

			for(; offset < page.getSize(); offset += skip){
				if(Utils.loadIntFromBytes(data, offset) == rowid){
					List<String> cols = null;
					entry = loadEntryFromBytes(data, szHdr, types, offset);
					break;
				}
			}
			return entry;
//		}else{
//			return entry;
//		}
	}
	/**
	 * 读取指定页面中的数据返回记录
	 * @param pgno 要写入数据的页号
	 * @return 指定页面的记录的Map表示 rowid，记录值
	 */
	public List<Map.Entry<Integer, String>> readRecord(int pgno){
		if(pgno <= 0 )
			return null;
		List<String> result = new ArrayList<String>();
		Page page  = aquirePage(pgno);
		int offset = page.getOffset();
		if(offset == page.getSize() || offset <= SpaceAllocation.PAGE_HEADER_SIZE)
			return null;
		byte[] data = page.getData();
		int hdrSz = Utils.loadIntFromBytes(data, offset + 4);
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
		List<Map.Entry<Integer, String>> list = new ArrayList<Map.Entry<Integer, String>>();

		for(int index = offset; index < page.getSize(); ){
			Map.Entry<Integer, String> entry = loadEntryFromBytes(data, hdrSz, types, index);
			list.add(entry);
			index += hdrSz + dataSize;
		}

		return list;
	}

	public String colsToRow(List<String> cols){
		StringBuilder row = new StringBuilder();
		for(String col : cols){
			row.append(col+",");
		}
		return row.substring(0,row.length()-1);
	}
	public void appendData(Page page, Map.Entry<Integer, byte[]> data){
		page.appendData(data);
		pCache.makeDirty(page);


	}
	public void updateHeader(Page page){
		pCache.makeDirty(page);
	}
	/**
	 * 从指定字节数组中加载一条entry（格式：rowid，行记录String）
	 * @param data		读取的来源
	 * @param types		类型数组
	 * @param hdrSz
	 * @param start		加载起始位置
	 * @return 一条记录对应的Entry
	 */
	private Map.Entry<Integer, String> loadEntryFromBytes(byte[] data, int hdrSz, int[] types, int start){
		Map<Integer, String> entry = new HashMap<Integer, String>();
		List<String> cols = new ArrayList<String>();
		int colNum = types.length;
		int rowid = Utils.loadIntFromBytes(data, start);
		start += hdrSz;
		for (int i = 0; i < colNum; i++) {
			switch (types[i]) {
				case DataType.INTEGER:
					cols.add(new Integer(Utils.loadIntFromBytes(data, start)).toString());
					start += 4;
					break;
				case DataType.SMALL_INT:
					cols.add(new Short(Utils.loadShortFromBytes(data, start)).toString());
					start += 2;
					break;
				case DataType.TINY_INT:
					cols.add(new Byte(data[start]).toString());
					start += 1;
					break;
				case DataType.TEXT:
					cols.add(Utils.loadStrFromBytes(data, start, 50));
					start += 50;
					break;
				case DataType.LONG:
					cols.add(new Long(Utils.loadLongFromBytes(data, start)).toString());
					start += 8;
					break;
			}
		}

		entry.put(rowid, colsToRow(cols));
		return entry.entrySet().iterator().next();
	}

	/**
	 * 刷新页面，写磁盘
	 */
	public void flush(){
		if(this.mxPgno < this.database.getDbSize()){
			truncate(this.mxPgno);
		}
		RandomAccessFile raf = null;
		try{
			raf = new RandomAccessFile(this.database.getDBFile(),"rw");
			for(int i = 0; i < pCache.getDirtyPgs().size(); i++)
			{
				List<Page> dirtyPgs = pCache.getDirtyPgs();
				for(Page page : dirtyPgs){
					if(page.getPgno() == 1){
						Utils.fillInt(this.mxPgno, page.getData(), Position.MAX_PGNO_IN_FIRST_PAGE);
					}
					raf.seek((page.getPgno()-1)*SpaceAllocation.PAGE_SIZE);
					raf.write(page.getData());
				}
			}
		}catch (IOException e){
			e.printStackTrace();
		}finally {
			try{
				if(raf != null) raf.close();
			}catch (IOException e){
				e.printStackTrace();
			}

		}
	}

	/**
	 *	根据页号，将数据加载到一个Page
	 * @param pgno 页号
	 * @return
	 */
	private Page loadPage(int pgno, Page newPage){
		if(pgno <= 0)
			return null;

		RandomAccessFile raf = null;

		try{
			raf = new RandomAccessFile(this.database.getDBFile(),"rw");
			raf.seek((pgno-1) * SpaceAllocation.PAGE_SIZE);

			byte[] data = new byte[SpaceAllocation.PAGE_SIZE];
			raf.read(data, 0 , SpaceAllocation.PAGE_SIZE);
			newPage.copyData(data);
			populatePageObj(newPage);
			return newPage;
		}catch (IOException e){
			e.printStackTrace();
		}finally {
			try{
				if(raf != null) raf.close();
			}catch (IOException e){
				e.printStackTrace();
			}
		}
		return newPage;
	}

	/**
	 * 根据page对象中的数据域（有效）对page对象进行初始化
	 * @param page
	 */
	private void populatePageObj(Page page){
		page.setPgno(Utils.loadIntFromBytes(page.getData(), Position.PGNO_IN_PAGE));
		if(page.getPgno() == 1)
			page.setTableCount(Utils.loadIntFromBytes(page.getData(), Position.TABLE_COUNT_IN_FIRST_PAGE));
		page.setPageType(page.getData()[Position.PGTYPE_IN_PAGE]);
		page.setOffset(Utils.loadIntFromBytes(page.getData(), Position.OFFSET_IN_PAGE));
		page.setOverflowPgno(Utils.loadIntFromBytes(page.getData(), Position.OVERFLOWPGNO_IN_PAGE));
		page.setpParent(Utils.loadIntFromBytes(page.getData(), Position.PARENT_PAGE_IN_PAGE));
		page.setpPrev(Utils.loadIntFromBytes(page.getData(), Position.PREV_PAGE_IN_PAGE));
		page.setpNext(Utils.loadIntFromBytes(page.getData(), Position.NEXT_PAGE_IN_PAGE));
		page.setOrder(page.getData()[Position.ORDER_IN_BPLUS_ROOT]);
		page.setHead(Utils.loadIntFromBytes(page.getData(), Position.HEAD_IN_BPLUS_ROOT));
		page.setMaxRowID(Utils.loadIntFromBytes(page.getData(), Position.MAX_ROWID_IN_BPLIS_ROOT));

		byte nCell = page.getData()[Position.CELLNUM_IN_PAGE];
		List<Integer> cells = new ArrayList<>();
		for(int i = 0 ; i<nCell; i++){
			page.addCell(Utils.loadIntFromBytes(page.getData(), Position.CELL_IN_PAGE + (i*4) ));
		}
	}

	/**
	 *
	 * @param nPage 新的页面大小
	 */
	public void truncate(int nPage){

	}

	/**
	 * 分配一个全新的页面给用户
	 * @return
	 */
	public Page newPage(){
//		Page page = aquirePage(this.mxPgno+1);
		Page page = new Page();
		page.setPgno(this.mxPgno+1);
		this.mxPgno ++;
		return page;
	}
	public void freePage(int pgno){
		pCache.free(pgno);
	}
//	private void resizePages()
//	{
//		int oldPageNum = this.pageNum;
//		Page[] newPages = new Page[oldPageNum * 2];
//		for(int i = 0; i< oldPageNum; i++){
//			newPages[i] = this.pages[i];
//		}
//		for(int i = oldPageNum; i < newPages.length; i++){
//			Page page = new Page();
//			page.setData(new byte[SpaceAllocation.PAGE_SIZE]);
//			page.setPgno(i);
//			page.setSectorSize(SpaceAllocation.SECTOR_SIZE);
//			page.setSize(SpaceAllocation.PAGE_SIZE);
//			page.setOffset(SpaceAllocation.PAGE_SIZE);
//			newPages[i] = page;
//		}
//
//		this.pages = newPages;
//		this.pageNum = newPages.length;
//	}
}
