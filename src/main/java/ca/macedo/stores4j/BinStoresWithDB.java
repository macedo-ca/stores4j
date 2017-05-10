/******************************************************************************
 *  Copyright (c) 2017 Johan Macedo
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  Contributors:
 *    Johan Macedo
 *****************************************************************************/
package ca.macedo.stores4j;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Consumer;

import javax.sql.DataSource;

public class BinStoresWithDB extends BinStores {
	protected static class DBObjects{
		protected String TABLE="BINARIES", NAME="FNAME", GROUP="FGROUP", FOLDER="FFOLDER", DATA="FDATA",
			   CREATED="FCREATED",UPDATED="FUPDATED",ID="FID",SIZE="FSIZE";
	}
	protected static DBObjects OBJ=new DBObjects();
	
	static BinStoresWithDB inst=new BinStoresWithDB();
	public static BinStore newDBStore(String conStr, String grp) {
		return inst.create(conStr,grp);
	}
	private DBBinStore create(String conStr, String grp){
		return new DBBinStore(conStr, grp);
	}
	public class DBBinStore extends BinStore{
		DBObjects n=OBJ;
		String fileGroup=null;
		DataSource dataSource=null;
		String conString=null;
		
		String folder=null;
		
		@Override
		public BinStore folder(String name) {
			DBBinStore sub=null;
			if(dataSource!=null){
				sub=new DBBinStore(dataSource,fileGroup);
			}else{
				sub=new DBBinStore(conString,fileGroup);
			}
			sub.folder=(folder!=null ? folder +"/":"") + name;
			return sub;
		}
		
		public DBBinStore(DataSource ds, String folder){
			this.fileGroup=folder;
			this.dataSource=ds;
		}
		public DBBinStore(String conStr, String folder){
			this.fileGroup=folder;
			// TODO do JNDI lookup
			this.conString=conStr;
			try(Connection con=getCon()){}catch(SQLException e){
				throw new RuntimeException(e);
			}
		}
		Connection getCon() throws SQLException{
			return dataSource!=null ? dataSource.getConnection() : DriverManager.getConnection(conString);
		}
		
		protected Collection<String> list(BinStoreVisit visit){
			return list0(((DBVisit)visit).con);
		}
		@Override
		public Collection<String> list(){
			try(Connection con = getCon()){
				return list0(con);
			}catch(SQLException e){
				throw new RuntimeException(e);
			}
		}
		
		private Collection<String> list0(Connection con) {
			LinkedList<String> out=new LinkedList<String>();
			PreparedStatement ps=null;
			ResultSet rs=null;
			try{
				String sql="SELECT "+n.NAME+" FROM "+n.TABLE+" WHERE "+n.GROUP+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"")+" ORDER BY "+n.NAME+"";
				ps=con.prepareStatement(sql);
				ps.setString(1, fileGroup);
				if(folder!=null) ps.setString(2, folder);
				rs=ps.executeQuery();
				while(rs.next()){
					out.add(rs.getString(1));
				}
			}catch(SQLException e){
				throw new RuntimeException(e);
			}finally{
				close(null,ps,rs);
			}
			return out;
		}
		@Override
		public boolean has(String id){
			try(Connection con = getCon()){
				return has0(con,id);
			}catch(SQLException e){
				throw new RuntimeException(e);
			}
		}
		boolean has0(Connection con, String id){
			PreparedStatement ps=null;
			ResultSet rs=null;
			try{
				String sql="SELECT ROWNUM FROM "+n.TABLE+" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
				ps=con.prepareStatement(sql);
				ps.setString(1, fileGroup);
				ps.setString(2, id);
				if(folder!=null) ps.setString(3, folder);
				rs=ps.executeQuery();
				return rs.next();
			}catch(SQLException e){
				throw new RuntimeException(e);
			}finally{
				close(null,ps,rs);
			}
		}
		
		@Override
		protected BinStoreVisit newVisit() {
			return new DBVisit();
		}
		private class DBVisit extends BinStoreVisit{
			Connection con=null;
			@Override
			protected void onBeforeVisit() {
				try {
					con=getCon();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			@Override
			protected void onAfterVisit() {
				close(con, null, null);
			}
		}
		@Override
		public boolean supportsLoading() {
			return true;
		}
		
		@Override
		public BinRef item(final String id) {
			return new BinRef() {
				String fileName=id;
				
				@Override
				public String getID() {
					return fileName;
				}
				@Override
				public byte[] getContent() throws IOException {
					PreparedStatement ps=null;
					ResultSet rs=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="SELECT "+n.DATA+" FROM "+n.TABLE+" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
						ps=con.prepareStatement(sql);
						ps.setString(1, fileGroup);
						ps.setString(2, fileName);
						if(folder!=null) ps.setString(3, folder);
						rs=ps.executeQuery();
						if(rs.next()){
							return rs.getBytes(1);
						}
					}catch(SQLException e){
						throw new IOException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,rs);
					}
					return null;
				}
				@Override
				public boolean delete() {
					PreparedStatement ps=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="DELETE FROM "+n.TABLE+" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
						ps=con.prepareStatement(sql);
						ps.setString(1, fileGroup);
						ps.setString(2, fileName);
						if(folder!=null) ps.setString(3, folder);
						int i=ps.executeUpdate();
						return i>0;
					}catch(SQLException e){
						throw new RuntimeException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
				}
				@Override
				public boolean rename(String newID) {
					PreparedStatement ps=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="UPDATE "+n.TABLE+" SET "+n.NAME+"=? WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
						ps=con.prepareStatement(sql);
						ps.setString(1, newID);
						ps.setString(2, fileGroup);
						ps.setString(3, fileName);
						if(folder!=null) ps.setString(3, folder);
						int i=ps.executeUpdate();
						return i>0;
					}catch(SQLException e){
						throw new RuntimeException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
				}
				@Override
				public InputStream getContentStream() throws IOException {
					PreparedStatement ps=null;
					ResultSet rs=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="SELECT "+n.DATA+" FROM "+n.TABLE+" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
						ps=con.prepareStatement(sql);
						ps.setString(1, fileGroup);
						ps.setString(2, fileName);
						if(folder!=null) ps.setString(3, folder);
						rs=ps.executeQuery();
						if(rs.next()){
							return rs.getBinaryStream(1);
						}
					}catch(SQLException e){
						throw new IOException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
					return null;
				}
				@Override
				public void copyTo(BinRef other) throws IOException {
					PreparedStatement ps=null;
					ResultSet rs=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="SELECT LENGTH("+n.DATA+") as LE,"+n.DATA+" FROM "+n.TABLE+" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
						ps=con.prepareStatement(sql);
						ps.setString(1, fileGroup);
						ps.setString(2, fileName);
						if(folder!=null) ps.setString(3, folder);
						rs=ps.executeQuery();
						if(rs.next()){
							int len=rs.getInt(1);
							InputStream is= rs.getBinaryStream(2);
							other.setContentStream(is, len);
						}
					}catch(SQLException e){
						throw new IOException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
				}
				@Override
				public boolean supportsLoading() {
					return true;
				}
				@Override
				public void consume(Consumer<LoadedBinary> consumer) throws IOException {
					LoadedBinary lb=new LoadedBinary();
					lb.id=fileName;
					lb.visit=visit;
					lb.store=DBBinStore.this;
					PreparedStatement ps=null;
					ResultSet rs=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="SELECT LENGTH("+n.DATA+") as LE,"+n.UPDATED+","+n.DATA+" FROM "+n.TABLE+" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
						ps=con.prepareStatement(sql);
						ps.setString(1, fileGroup);
						ps.setString(2, fileName);
						if(folder!=null) ps.setString(3, folder);
						rs=ps.executeQuery();
						if(rs.next()){
							lb.setLength(rs.getLong(1));
							Timestamp ts=rs.getTimestamp(2);
							lb.setLastModified(ts!=null ? ts.getTime() : null);
							lb.setInputStream(rs.getBinaryStream(3));
							consumer.accept(lb);
						}
					}catch(SQLException e){
						throw new IOException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
				}
				@Override
				public Long getLength() {
					PreparedStatement ps=null;
					ResultSet rs=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="SELECT LENGTH("+n.DATA+") as LE FROM "+n.TABLE+" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
						ps=con.prepareStatement(sql);
						ps.setString(1, fileGroup);
						ps.setString(2, fileName);
						if(folder!=null) ps.setString(3, folder);
						rs=ps.executeQuery();
						if(rs.next()){
							long len=rs.getLong(1);
							return len;
						}
						return null;
					}catch(SQLException e){
						throw new RuntimeException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
				}
				@Override
				public Long getLastModified() {
					PreparedStatement ps=null;
					ResultSet rs=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="SELECT "+n.UPDATED+" FROM "+n.TABLE+" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":"");
						ps=con.prepareStatement(sql);
						ps.setString(1, fileGroup);
						ps.setString(2, fileName);
						if(folder!=null) ps.setString(3, folder);
						rs=ps.executeQuery();
						if(rs.next()){
							Timestamp len=rs.getTimestamp(1);
							return len.getTime();
						}
						return null;
					}catch(SQLException e){
						throw new RuntimeException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
				}
				@Override
				public boolean createWithContent(byte[] content) throws IOException {
					PreparedStatement ps=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						String sql="INSERT INTO "+n.TABLE+" ("+n.CREATED+","+n.UPDATED+","+n.ID+","+n.GROUP+","+n.NAME+","+n.SIZE+","+n.DATA+""+(folder!=null?","+n.FOLDER+"":"")+") VALUES (SYSDATE,SYSDATE,?,?,?,?,?"+(folder!=null?",?":"")+")";
						ps= con.prepareStatement(sql);
						ps.setString(1, guid(32));
						ps.setString(2, fileGroup);
						ps.setString(3, fileName);
						ps.setInt(4, content.length);
						ps.setBytes(5, content);
						if(folder!=null) ps.setString(6, folder);
						int out =ps.executeUpdate();
						return out>0;
					}catch(SQLException e){
						throw new IOException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}				
				}
				@Override
				public void setContent(byte[] content)throws IOException {
					PreparedStatement ps=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						if(has0(con,id)){
							ps= con.prepareStatement("UPDATE "+n.TABLE+" SET "+n.UPDATED+"=SYSDATE, "+n.DATA+"=?, "+n.SIZE+"=? WHERE "+n.GROUP+"=? AND "+n.NAME+"=? "+(folder!=null?" AND "+n.FOLDER+"=?":""));
							ps.setBytes(1, content);
							ps.setInt(2, content.length);
							ps.setString(3, fileGroup);
							ps.setString(4, fileName);
							if(folder!=null) ps.setString(5, folder);
							ps.executeUpdate();
						}else{
							String sql="INSERT INTO "+n.TABLE+" ("+n.CREATED+","+n.UPDATED+","+n.ID+","+n.GROUP+","+n.NAME+","+n.SIZE+","+n.DATA+""+(folder!=null?","+n.FOLDER+"":"")+") VALUES (SYSDATE,SYSDATE,?,?,?,?,?"+(folder!=null?",?":"")+")";
							ps= con.prepareStatement(sql);
							ps.setString(1, guid(32));
							ps.setString(2, fileGroup);
							ps.setString(3, fileName);
							ps.setInt(4, content.length);
							ps.setBytes(5, content);
							if(folder!=null) ps.setString(6, folder);
							ps.executeUpdate();
						}
					}catch(SQLException e){
						throw new IOException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
				}
				@Override
				public OutputStream setContentStream() throws IOException {
					PreparedStatement ps=null;
					ResultSet rs=null;
					Connection con = null;
					try{
						con=inVisit() ? visitAs(DBVisit.class).con : getCon();
						OutputStream os = null;
						java.sql.Blob binary = null;
						
						String readBLOB="SELECT "+n.DATA+" " +
						" FROM "+n.TABLE+" " +
						" WHERE "+n.GROUP+"=? AND "+n.NAME+"=? " +(folder!=null?" AND "+n.FOLDER+"=?":"") +
						" FOR UPDATE";
						ps = con.prepareStatement(readBLOB);
						ps.setString(1, fileGroup);
						ps.setString(2, fileName);
						if(folder!=null) ps.setString(3, folder);
						rs = ps.executeQuery();
						if(!rs.next()){
							BinStoresWithDB.close(null,ps,rs);
							String sql="INSERT INTO "+n.TABLE+" ("+n.CREATED+","+n.UPDATED+","+n.ID+","+n.GROUP+","+n.NAME+","+n.SIZE+","+n.DATA+"" +(folder!=null?","+n.FOLDER+"":"")+") VALUES (SYSDATE,SYSDATE,?,?,?,0,EMPTY_BLOB()" +(folder!=null?",?":"")+")";
							ps= con.prepareStatement(sql);
							ps.setString(1, guid(32));
							ps.setString(2, fileGroup);
							ps.setString(3, fileName);
							if(folder!=null) ps.setString(4, folder);
							ps.executeUpdate();
							ps.close();
							ps = con.prepareStatement(readBLOB);
							ps.setString(1, fileGroup);
							ps.setString(2, fileName);
							if(folder!=null) ps.setString(3, folder);
							rs = ps.executeQuery();
							rs.next();
						}
						binary = rs.getBlob(1);
						final Connection lclCon=(!inVisit() ? con : null);
						final PreparedStatement lclPS=ps;
						final ResultSet lclRS=rs;
						os = new BufferedOutputStream(binary.setBinaryStream(0)){
							@Override
							public void close() throws IOException {
								super.close();
								BinStoresWithDB.close(lclCon,lclPS,lclRS);
							}
						};
						return os;
					} catch (SQLException e) {
						throw new IOException(e);
					}finally{
						if(inVisit()) con=null;
						close(con,ps,null);
					}
				}
				private String guid(int i) {
					// TODO fix
					return "T"+System.nanoTime()+""+((int)(Math.random()*1000));
				}
			};
		}
	}
	public static void close(Connection con,PreparedStatement ps, ResultSet rs) {
		try{if(rs!=null)rs.close();}catch(Throwable t){}
		try{if(ps!=null)ps.close();}catch(Throwable t){}
		try{if(con!=null)con.close();}catch(Throwable t){}
	}

	
	
}
