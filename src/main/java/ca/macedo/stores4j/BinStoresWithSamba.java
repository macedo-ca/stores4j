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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

public class BinStoresWithSamba extends BinStores{
	@Override
	protected BinStore extendedGetStore(BinStores mainStores, String uri) {
		if(uri.toLowerCase().startsWith("smb://")){
			try{
				if(!uri.contains("@")){
					uri=uri.substring("smb://".length());
					String def=mainStores.defaultCredentials("smb");
					if(def==null || def.trim().length()==0) throw new RuntimeException("smb requires credentials");
					uri ="smb://"+def+"@"+uri;
				}
				return new SmbFileStore(uri,new SmbFile(uri));
			} catch (Throwable e) {
				sambalog.error("Error accessing Samba folder: "+uri,e);
			}
		}
		return null;
	}
	
	private static Logger sambalog=LoggerFactory.getLogger(SmbFileStore.class);
	public class SmbFileStore extends BinStore{
		public SmbFileStore(String uri,SmbFile folder){
			this.uri=uri;
			if(uri.endsWith("/")) uri=uri.substring(0, uri.length()-1);
			this.folder=folder;
			try {
				if(!folder.exists()) folder.mkdirs();
			} catch (SmbException e) {
				sambalog.error("Error accessing Samba folder: "+folder,e);
			}
		}
		String uri=null;
		SmbFile folder=null;
		
		@Override
		public BinStore folder(String name) {
			String newURI=uri+name;
			try{
				return new SmbFileStore(newURI,new SmbFile(newURI));
			} catch (Throwable e) {
				sambalog.error("Error accessing Samba folder: "+uri,e);
			}
			return null;
		}
		@Override
		public Collection<String> filter(String filter) {
			LinkedList<String> out=new LinkedList<String>();
			try{
				SmbFile[] files=null;
				Filter flt=prepareFilter("", filter);
				if(flt.all()){
					files=folder.listFiles();
				}else{
					files=folder.listFiles(flt.wildCardFilter());
				}
				for(SmbFile f : files) out.add(f.getName());
			} catch (SmbException e) {
				sambalog.error("Error accessing Samba folder: "+folder,e);
			}
			return out;
		}
		@Override
		public Collection<String> list() {
			LinkedList<String> out=new LinkedList<String>();
			try{
				SmbFile[] files=null;
				if(baseFilter.all()){
					files=folder.listFiles();
				}else{
					files=folder.listFiles(baseFilter.wildCardFilter());
				}
				for(SmbFile f : files) out.add(f.getName());
			} catch (SmbException e) {
				sambalog.error("Error accessing Samba folder: "+folder,e);
			}
			return out;
		}
		@Override
		public boolean has(String id) {
			try{
				return new SmbFile(folder,id).exists();
			} catch (Throwable e) {
				sambalog.error("Error accessing Samba folder: "+folder,e);
				return false;
			}
		}
		@Override
		public BinRef item(final String id) {
			return new BinRef() {
				SmbFile file=null;
				{
					try{
						file=new SmbFile(folder,id);
					} catch (Throwable e) {
						sambalog.error("Error accessing Samba folder: "+folder,e);
					}
				}
				
				@Override
				public String getID() {
					return id;
				}
				@Override
				public byte[] getContent()throws IOException {
					ByteArrayOutputStream bos=new ByteArrayOutputStream();
					IOUtils.write(getContent(), bos);
					bos.flush();
					return bos.toByteArray();
				}
				@Override
				public InputStream getContentStream() throws IOException {
					return file.getInputStream();
				}
				@Override
				public void copyTo(BinRef other) throws IOException {
					try(InputStream data=file.getInputStream()){
						int len=file.getContentLength();
						other.setContentStream(data, len);
					}
				}
				@Override
				public void setContent(byte[] content)throws IOException {
					try (SmbFileOutputStream fw=new SmbFileOutputStream(file)){
						fw.write(content);
					}
				}
				@Override
				public boolean createWithContent(byte[] content) throws IOException {
					if(has(id)) return false;
					setContent(content);
					return true;
				}
				@Override
				public OutputStream setContentStream() throws IOException {
					return new SmbFileOutputStream(file,false);
				}
				@Override
				public boolean delete() {
					try {
						file.delete();
						return true;
					} catch (SmbException e) {
						return false;
					}
				}
				@Override
				public boolean rename(String newID) {
					try {
						file.renameTo(new SmbFile(folder,newID));
						return true;
					} catch (Throwable e) {
						return false;
					}
				}
			};
		}
	}
}
