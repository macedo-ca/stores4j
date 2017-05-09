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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.macedo.stores4j.BinStores.BinStore.BinStoreVisit;

/**
 * Generic binary storage API for multiple types of backends
 */
public class BinStores {
	private static Logger log=LoggerFactory.getLogger(BinStores.class);
	static Charset UTF_8=Charset.forName("UTF-8");
	
	static BinStores sambaFactory=null;
	static BinStores s3Factory=null;
	static{
		try {
			sambaFactory=(BinStores)Thread.currentThread().getContextClassLoader().loadClass(BinStores.class.getName()+"WithSamba").newInstance();
		} catch (Throwable e) {
			log.info("Samba stores are not available "+(e.getMessage()));
			log.debug("Reason that samba is not being available is ",e);
		}
		try {
			s3Factory=(BinStores)Thread.currentThread().getContextClassLoader().loadClass(BinStores.class.getName()+"WithS3").newInstance();
		} catch (Throwable e) {
			log.info("AWS S3 stores are not available "+(e.getMessage()));
			log.debug("Reason that AWS S3 is not being available is ",e);
		}
	}
	public static Extensions<BinStore,BinStores> EXTENSIONS=new Extensions<BinStore,BinStores>();
	
	public BinStores(){}
	
	LinkedList<WeakReference<Function<String,String>>> secretSources=new LinkedList<WeakReference<Function<String,String>>>();
	HashMap<String,String> cred=new HashMap<String,String>();
	{
		addSecretsSource(cred::get);
	}
	
	public BinStore getStore(String uri){
		log.info("Resolving " + uri);
		if(uri.startsWith("store:")) uri=after(uri, "store:");
		if(secretSources.size()>0) uri=applySecrets(uri);
		return internalGetStore(uri);
	}
	protected BinStore extendedGetStore(BinStores mainStores, String uri) {
		// noop
		return null;
	}
	BinStore internalGetStore(String uri) {
		if(uri.toLowerCase().startsWith("file:")){
			uri=after(uri, "file:");
			File folder=new File(uri);
			return new FileStore(folder).initialize(this);
		}else if(uri.toLowerCase().startsWith("mem:")){
			uri=after(uri, "mem:");
			MemoryStore ms= memStores.get(uri);
			if(ms==null){
				memStores.put(uri, ms=new MemoryStore(uri));
			}
			return ms.initialize(this);
		}else if(uri.toLowerCase().startsWith("db:")){
			uri=uri.toLowerCase().startsWith("db://") ? after(uri, "db://"): after(uri, "db:");
			if(uri.contains(":")){
				String grp = uri.substring(0, uri.indexOf(':'));
				String conStr = uri.substring(uri.indexOf(':')+1);
				return BinStoresWithDB.newDBStore(conStr, grp).initialize(this);
			}else{
				String grp=uri;
				String conStr = defaultCredentials("db");
				return BinStoresWithDB.newDBStore(conStr, grp).initialize(this);
			}
		}else if(uri.toLowerCase().startsWith("smb:")){
			if(sambaFactory!=null){
				return sambaFactory.extendedGetStore(this,uri).initialize(this);
			}else{
				log.error("Samba is not available");
			}
		}else if(uri.toLowerCase().startsWith("s3:")){
			if(s3Factory!=null){
				return s3Factory.extendedGetStore(this,uri).initialize(this);
			}else{
				log.error("AWS S3 is not available");
			}
		}
		return EXTENSIONS.tryCreate(this, uri);
	}
	protected static String after(String text, String after) {
        if (!text.contains(after)) {
            return null;
        }
        return text.substring(text.indexOf(after) + after.length());
    }
	
	public BinStores setDefaultCredentials(String storeType, String credentials){
		cred.put("DEFAULT:"+storeType.toUpperCase(), credentials);
		return this;
	}
	public BinStores addSecretsSource(Function<String,String> f){
		secretSources.add(new WeakReference<Function<String,String>>(f));
		return this;
	}
	
	static String secretKeyWord="SECRET";
	String applySecrets(String s){
		if(s==null || s.length()<secretKeyWord.length()) return s;
		int idx=s.indexOf(secretKeyWord+"(");
		if(idx>-1){
			int idxEnd =s.indexOf(')', idx);
			if(idxEnd>-1){
				String before=s.substring(0,idx);
				String after=s.substring(idxEnd+1);
				String secretName=s.substring(idx+(secretKeyWord+"(").length(),idxEnd);
				String secretValue=findSecretValue(secretName);
				s = before + secretValue + after;
				return applySecrets(s);
			}
		}
		return s;
	}
	private String findSecretValue(String secretName){
		for(WeakReference<Function<String,String>> r : secretSources){
			Function<String,String> f=r.get();
			if(f!=null){
				String val=f.apply(secretName);
				if(val!=null){
					return val;
				}
			}
		}
		return null;
	}
	protected String defaultCredentials(String storeType){
		return findSecretValue("DEFAULT:"+storeType.toUpperCase());
	}
	
	protected interface PartOfVisit{
		public void runWithin(BinStoreVisit visit) throws Exception;
	}
	public int getLargeSize() {
		return largeSize;
	}
	public void setLargeSize(int largeSize) {
		this.largeSize = largeSize;
	}

	int largeSize=1024*32;
	
	public static abstract class BinStore extends BaseStore{
		int largeSize=1024*32;
		protected BinStore initialize(BinStores stores){
			this.largeSize=stores.largeSize;
			return this;
		}
		
		public abstract BinStore folder(String name);
		public abstract Collection<String> list();
		public abstract boolean has(String id);
		public Collection<String> filter(String filter) {
			return prepareFilter("",filter).filter(list());
		}
		public void forEachIDAndContent(BiConsumer<String,byte[]> r){
			try{
				runVisit((visit)->{
					for(String s : list()){
						BinRef ref=item(visit,s);
						r.accept(ref.getID(), ref.getContent());
					}
				});
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		public void forEachIDAndStream(BiConsumer<String,InputStream> r){
			try{
				runVisit((visit)->{
					for(String s : list()){
						BinRef ref=item(visit,s);
						r.accept(ref.getID(), ref.getContentStream());
					}
				});
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		protected Collection<String> list(BinStoreVisit visit){
			return list();
		}
		protected void forEach0(BinStoreVisit visit, Consumer<BinRef> r){
			for(String s : list(visit)){
				r.accept(item(visit,s));
			}
		}
		public void forEach(Consumer<BinRef> r){
			try{
				runVisit((visit)->{
					forEach0(visit,r);
				});
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		public void forEachFiltered(Consumer<BinRef> r, String filter){
			try{
				runVisit((visit)->{
					for(String s : filter(filter)){
						r.accept(item(visit,s));
					}
				});
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		public void replicateTo(BinStore store) throws IOException{
			try{
				runVisit((visit)->{
					store.runVisit((storeVisit)->{
						Collection<String> list=list(visit);
						for(String s : list){
							item(visit,s).copyTo(store.item(storeVisit,s));
						}
						ArrayList<BinRef> deletes=new ArrayList<>();
						store.forEach0(storeVisit, existingRec->{
							if(!list.contains(existingRec.getID())){
								deletes.add(existingRec);
							}
						});
						if(deletes.size()>0){
							for(BinRef r : deletes) r.delete();
						}
					});
				});
			}catch(IOException e){
				throw e;
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		public void copyTo(BinStore store) throws IOException{
			try{
				runVisit((visit)->{
					store.runVisit((storeVisit)->{
						Collection<String> list=list(visit);
						for(String s : list){
							item(visit,s).copyTo(store.item(storeVisit,s));
						}
					});
				});
			}catch(IOException e){
				throw e;
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		public void copyTo(BinStore store, String filter) throws IOException{
			try{
				runVisit((visit)->{
					store.runVisit((storeVisit)->{
						Collection<String> list=filter(filter);
						for(String s : list){
							store.item(storeVisit,s).setContent(item(visit,s).getContent());
						}
					});
				});
			}catch(IOException e){
				throw e;
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		public void list(Consumer<String> con){
			try{
				runVisit((visit)->{
					for(String s : list()){
						con.accept(s);
					}
				});
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		protected void runVisit(PartOfVisit r) throws Exception{
			BinStoreVisit c=newVisit();
			try{
				c.onBeforeVisit();
				r.runWithin(c);
			}finally{
				c.onAfterVisit();
			}
		}
		protected BinStoreVisit newVisit(){
			return new BinStoreVisit();// Base noop
		}
		protected class BinStoreVisit{
			protected void onBeforeVisit(){
				// noop
			}
			protected void onAfterVisit(){
				// noop
			}
		}
		
		public void items(Consumer<BinRef> consumer, String ... ids){
			try{
				runVisit((visit)->{
					for(String id : ids){
						consumer.accept(item(visit,id));
					}
				});
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		public void item(String id, Consumer<BinRef> consumer){
			try{
				runVisit((visit)->{
					consumer.accept(item(visit,id));
				});
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		protected BinRef item(BinStoreVisit visit, String id){
			BinRef out=item(id);
			out.visit=visit;
			return out;
		}
		public boolean supportsLoading(){
			return false;
		}
		
		public abstract BinRef item(String id);
		public abstract class BinRef{
			@SuppressWarnings("unchecked")
			protected <T extends BinStoreVisit> T visitAs(Class<T> clz){
				return (T)visit;
			}
			protected boolean inVisit(){
				return visit!=null;
			}
			public class LoadedBinary extends MetaData{
				BinStoreVisit visit=null;
				InputStream is=null;
				public InputStream getInputStream() {
					return is;
				}
				public void setInputStream(InputStream is) {
					this.is = is;
				}
			}
			public boolean supportsLoading(){
				return false;
			}
			public void load(Consumer<LoadedBinary> consumer) throws IOException {
				throw new RuntimeException("Not supported by "+this.getClass().getName());
			}
			
			BinStoreVisit visit=null;
			public abstract String getID();
			public  InputStream getContentStream() throws IOException{
				return new ByteArrayInputStream(getContent());
			}
			public long getLength(){
				throw new RuntimeException("Not supported by "+this.getClass().getName());
			}
			public long getLastModified(){
				throw new RuntimeException("Not supported by "+this.getClass().getName());
			}
			public String getETag(){
				throw new RuntimeException("Not supported by "+this.getClass().getName());
			}
			
			public abstract byte[] getContent() throws IOException;
			public abstract void setContent(byte[] content) throws IOException;
			public void setContentStream(InputStream st, Integer lengthIfKnown) throws IOException{
				try(OutputStream os=setContentStream()){
					if(lengthIfKnown!=null && lengthIfKnown>(largeSize)){
						IOUtils.copyLarge(st, os,0,lengthIfKnown,new byte[largeSize]);
					}else{
						IOUtils.copy(st, os);
					}
				}
			}
			public void copyTo(BinRef other) throws IOException{
				byte[] data=getContent();
				if(data.length<10000){
					other.setContent(data);
				}else{
					other.setContentStream(new ByteArrayInputStream(data),data.length);
				}
			}
			public OutputStream setContentStream() throws IOException{
				return new ByteArrayOutputStream(){
					@Override
					public void close() throws IOException {
						setContent(toByteArray());
						super.close();
					}
				};
			}
			public abstract boolean delete();
			public abstract boolean rename(String newID);
			public abstract boolean createWithContent(byte[] content) throws IOException;
		}
	}
	
	HashMap<String,MemoryStore> memStores=new HashMap<String,MemoryStore>();
	public class MemoryStore extends BinStore{
		String mementry=null;
		MemoryStore(String mementry){
			this.mementry=mementry;
		}
		ConcurrentHashMap<String,byte[]> map=new ConcurrentHashMap<String,byte[]>();
		
		@Override
		public BinStore folder(String name) {
			return getStore("mem:"+mementry+"/"+name);
		}
		
		@Override
		public Collection<String> list() {
			if(baseFilter.all()) return map.keySet();
			Collection<String> out=new ArrayList<>();
			for(String s : map.keySet()) if(baseFilter.match(s)) out.add(s);
			return out;
		}
		@Override
		public boolean has(String id) {
			return map.containsKey(id);
		}
		@Override
		public BinRef item(String id) {
			return new BinRef(){
				@Override
				public String getID() {
					return id;
				}
				@Override
				public byte[] getContent() {
					return map.get(id);
				}
				@Override
				public void setContent(byte[] content) {
					map.put(id,content);
				}
				public boolean delete(){
					return map.remove(id)!=null;
				}
				public boolean rename(String newID){
					byte[] v=map.remove(id);
					if(v!=null){
						map.put(newID, v);
						return true;
					}else{
						return false;
					}
				}
				public boolean createWithContent(byte[] content){
					if(map.containsKey(id)) return false;
					Object o = map.putIfAbsent(id, content);
					return o==null;
				}
				@Override
				public long getLength() {
					return getContent()!=null ? getContent().length : 0L;
				}
			};
		}
	}
	
	public class FileStore extends BinStore{
		public FileStore(File folder){
			this.folder=folder;
			if(!folder.exists()) folder.mkdirs();
		}
		File folder=null;
		
		@Override
		public BinStore folder(String name) {
			return new FileStore(new File(folder,name));
		}
		
		@Override
		public Collection<String> list() {
			LinkedList<String> out=new LinkedList<String>();
			for(File f : folder.listFiles()) if(baseFilter.match(f.getName())) out.add(f.getName());
			return out;
		}
		@Override
		public boolean has(String id) {
			return new File(folder,id).exists();
		}
		@Override
		public BinRef item(final String id) {
			return new BinRef() {
				File file=new File(folder,id);
				@Override
				public String getID() {
					return id;
				}
				@Override
				public byte[] getContent()throws IOException {
					return Files.readAllBytes(Paths.get(file.getAbsolutePath()));
				}
				@Override
				public InputStream getContentStream() throws IOException {
					return new FileInputStream(file);
				}
				@Override
				public void setContent(byte[] content)throws IOException {
					File prnt=file.getParentFile();
					if(!prnt.exists()) prnt.mkdirs();
					try (FileOutputStream fw=new FileOutputStream(file)){
						fw.write(content);
					}
				}
				@Override
				public void copyTo(BinRef other) throws IOException {
					int len=(int)file.length();
					other.setContentStream(getContentStream(), len);
				}
				@Override
				public OutputStream setContentStream() throws IOException {
					File prnt=file.getParentFile();
					if(!prnt.exists()) prnt.mkdirs();
					return new FileOutputStream(file,false);
				}
				@Override
				public boolean delete() {
					return file.delete();
				}
				@Override
				public boolean rename(String newID) {
					return file.renameTo(new File(folder,newID));
				}
				@Override
				public boolean createWithContent(byte[] content) throws IOException{
					if(file.exists()) return false;
					setContent(content);
					return true;
				}
				@Override
				public long getLastModified() {
					return file.lastModified();
				}
				@Override
				public long getLength() {
					return file.length();
				}
			};
		}
	}
}