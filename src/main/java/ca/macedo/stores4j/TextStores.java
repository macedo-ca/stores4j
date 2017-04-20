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

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.macedo.stores4j.BinStores.BinStore;
import ca.macedo.stores4j.BinStores.BinStore.BinRef;
import ca.macedo.stores4j.StoresJSONBuilder.JSONArrRoot;
import ca.macedo.stores4j.StoresJSONBuilder.JSONRoot;
import ca.macedo.stores4j.TextStores.JsonStore.JsonRef;
import ca.macedo.stores4j.TextStores.TextStore.TextRef;

/**
 * Reads and writes text files from abstract stores, either a local folder, Redis 
 */
public class TextStores {
	private static Logger log=LoggerFactory.getLogger(TextStores.class);
	static Charset UTF_8=Charset.forName("UTF-8");
	
	public static Extensions<TextStore,TextStores> EXTENSIONS=new Extensions<TextStore,TextStores>();
	static{
		try{
			Class.forName("ca.macedo.util1.redis.Redis"); // Tries to load redis library to get the Redis text-store started
		}catch(Throwable t){
			log.info("Redis text-store is not available");
		}
	}
	
	BinStores binStores=new BinStores();
	
	public TextStores(){}
	
	@SuppressWarnings("rawtypes")
	BiFunction<Class,Object,Object> factory=(clz,m)->{
		try {
			return clz.newInstance();
		} catch (InstantiationException e) {
			return null;
		} catch (IllegalAccessException e) {
			return null;
		}
	};
	@SuppressWarnings("rawtypes")
	public BiFunction<Class,Object,Object> getFactory() {
		return factory;
	}
	@SuppressWarnings("rawtypes")
	public void setFactory(BiFunction<Class,Object,Object> factory) {
		this.factory = factory;
	}
	public TextStores addSecretsSource(Function<String,String> f){
		binStores.addSecretsSource(f);
		return this;
	}
	
	public BinStores getBinStores() {
		return binStores;
	}
	public TextStores setBinStores(BinStores binStores) {
		this.binStores = binStores;
		return this;
	}

	public static TextRef findIn(String itemID, TextStore ...stores){
		for(TextStore ts : stores ){
			if(ts.has(itemID)) return ts.item(itemID);
		}
		return null;
	}
	
	public JsonStore getJsonStore(String uri){
		return new JsonStore(getStore(uri));
	}
	public TextStore getStore(String uri){
		log.info("Resolving " + uri);
		if(uri.startsWith("store:")) uri=after(uri, "store:");
		if(binStores.secretSources.size()>0) uri=binStores.applySecrets(uri);
		if(uri.toLowerCase().startsWith("file:")){
			uri=after(uri, "file:");
			File folder=new File(uri);
			return new FileStore(folder);
		}else if(uri.toLowerCase().startsWith("mem:")){
			uri=after(uri, "mem:");
			MemoryStore ms= memStores.get(uri);
			if(ms==null){
				memStores.put(uri, ms=new MemoryStore());
			}
			return ms;
		}else{
			TextStore out= EXTENSIONS.tryCreate(this, uri);
			if(out==null){
				BinStore store=binStores.internalGetStore(uri);
				if(store!=null){
					return new BinTextStore(store);
				}
			}
			return out;
		}
	}
	public static String after(String text, String after) {
        if (!text.contains(after)) {
            return null;
        }
        return text.substring(text.indexOf(after) + after.length());
    }
	@SuppressWarnings("unchecked")
	public static class JsonStore implements Closeable{
		TextStore store=null;
		boolean statusCodes=false;
		boolean validateJsonInput=true;
		public JsonStore noInputValidation(){
			validateJsonInput=false;
			return this;
		}
		public JsonStore returnStatusCodes(){
			statusCodes=true;
			return this;
		}
		public void close(){
			store.close();
		}
		public JsonStore(TextStore st){
			store=st;
		}
		public JSONArray list(){
			JSONArray out=new JSONArray();
			for(String id : store.list()) out.add(id);
			return out;
		}
		public JSONObject has(String id){
			JSONObject o=new JSONObject();
			boolean found=store.has(id);
			o.put("id", id);
			o.put("status", found?"exists":"notFound");
			if(statusCodes){
				o.put("status_code", found ? 200 : 404);
			}
			return o;
		}
		public JSONArray filter(String filter) {
			JSONArray out=new JSONArray();
			for(String id : store.filter(filter)) out.add(id);
			return out;
		}
		public JsonRef item(String id){
			JsonRef r=new JsonRef(store.item(id));
			if(statusCodes) r.returnStatusCodes();
			if(!validateJsonInput) r.noInputValidation();
			return r;
		}
		public static class JsonRef{
			boolean validateJsonInput=true;
			public JsonRef noInputValidation(){
				validateJsonInput=false;
				return this;
			}
			boolean statusCodes=false;
			public JsonRef returnStatusCodes(){
				statusCodes=true;
				return this;
			}
			JsonRef(TextRef r){
				item=r;
			}
			public JSONRoot build(){
				return StoresJSONBuilder.object(this::setContent);
			}
			public JSONArrRoot buildArr(){
				return StoresJSONBuilder.array(this::setContent);
			}
			TextRef item=null;
			public JSONObject delete(String id){
				JSONObject o=new JSONObject();
				o.put("id", id);
				if(item.delete(id)){
					o.put("status", "deleted");
					if(statusCodes){
						o.put("status_code", 200);
					}
				}else{
					o.put("status", "NotFound");
					if(statusCodes){
						o.put("status_code", 404);
					}
				}
				return o;
			}
			public JSONObject rename(String newID){
				String oldID=item.getID();
				JSONObject o=new JSONObject();
				o.put("id", newID);
				o.put("oldId", oldID);
				if(item.rename(newID)){
					o.put("status", "renamed");
					if(statusCodes){
						o.put("status_code", 200);
					}
				}else{
					o.put("status", "NotFound");
					if(statusCodes){
						o.put("status_code", 404);
					}
				}
				return o;
			}
			public JSONObject getID(){
				JSONObject o=new JSONObject();
				o.put("id", item.getID());
				if(statusCodes){
					o.put("status_code", 200);
				}
				return o;
			}
			public JSONObject getContent(){
				JSONObject o=new JSONObject();
				o.put("id", item.getID());
				String content=item.getContent();
				try {
					if(content!=null && content.length()>0 && (content.trim().startsWith("{") || content.trim().startsWith("["))){
						Object obj = new JSONParser().parse(content);
						o.put("content", obj);
						if(statusCodes){
							o.put("status_code", 200);
						}
					}else if(content==null || content.trim().length()==0){
						if(item.exist()){
							o.put("contentNull", true);
							if(statusCodes){
								o.put("status_code", 200);
							}
						}else{
							o.put("status", "NotFound");
							if(statusCodes){
								o.put("status_code", 404);
							}
						}
					}
				} catch (ParseException e) {
					o.put("contentString", item.getContent());
					if(statusCodes){
						o.put("status_code", 200);
					}
				}
				return o;
			}
			public JSONObject setContent(Object content){
				JSONObject o=new JSONObject();
				o.put("id", item.getID());
				String type=null;
				if(content instanceof InputStream){
					try {
						content = IOUtils.toString((InputStream)content, UTF_8);
					} catch (IOException e) {
						content=null;
						o.put("status", "failed");
						o.put("type", "unknown");
						if(statusCodes){
							o.put("status_code", 500);
						}
						return o;
					}
				}else if(content instanceof Reader){
					try {
						content = IOUtils.toString((Reader)content);
					} catch (IOException e) {
						content=null;
						o.put("status", "failed");
						o.put("type", "unknown");
						if(statusCodes){
							o.put("status_code", 500);
						}
						return o;
					}
				}
				if(content==null){
					if(validateJsonInput){
						o.put("status", "NotValidJSON");
						if(statusCodes){
							o.put("status_code", 406);
						}
						return o;
					}
					type="null";
					item.setContent((String)null);
				}else if(content instanceof JSONObject){
					type="object";
					item.setContent(((JSONObject)content).toJSONString());
				}else if(content instanceof JSONArray){
					type="array";
					item.setContent(((JSONArray)content).toJSONString());
				}else if(content instanceof String){
					if(validateJsonInput){
						Object data=null;
						try {
							data = new JSONParser().parse(content.toString());
						} catch (ParseException e) {
							o.put("status", "NotValidJSON");
							if(statusCodes){
								o.put("status_code", 406);
							}
							return o;
						}
						if(data instanceof JSONObject){
							type="object";
							item.setContent((String)content);
						}else if(data instanceof JSONArray){
							type="array";
							item.setContent((String)content);
						}
					}else{
						type="string";
						item.setContent(content.toString());
					}
					o.put("status", "saved");
				}
				if(validateJsonInput) o.put("type", type);
				if(statusCodes){
					o.put("status_code", 200);
				}
				return o;
			}
		}
	}
	public static abstract class TextStore implements Closeable{
		String suffix=null;
		public void close(){
			// noop
		}
		public abstract Collection<String> list();
		public abstract boolean has(String id);
		public Collection<String> filter(String filter) {
			LinkedList<String> out=new LinkedList<String>();
			for(String k : list()) if(k.startsWith(filter)) out.add(k);
			return out;
		}
		public void copyTo(TextStore store){
			Collection<String> list=list();
			for(String s : list){
				store.item(s).setContent(item(s).getContent());
			}
		}
		public void copyTo(TextStore store, String filter){
			Collection<String> list=filter(filter);
			for(String s : list){
				store.item(s).setContent(item(s).getContent());
			}
		}
		public abstract class TextRef{
			public boolean exist(){
				return TextStore.this.has(getID());
			}
			public abstract boolean delete(String delete);
			public abstract boolean rename(String newID);
			public abstract String getID();
			public abstract String getContent();
			public abstract boolean createWithContent(String content);
			public abstract void setContent(String content);
			public void setContent(InputStream in){
				try {
					setContent(IOUtils.toString(in, UTF_8));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		public JsonRef jsonItem(String id){
			return new JsonRef(item(id));
		}
		public abstract TextRef item(String id);
	}
	
	public class BinTextStore extends TextStore{
		BinStore store=null;
		BinTextStore(BinStore store){
			this.store=store;
		}
		@Override
		public Collection<String> list() {
			return store.list();
		}
		@Override
		public boolean has(String id) {
			return store.has(id);
		}
		@Override
		public TextRef item(String id) {
			return new TextRef(){
				BinRef ref=store.item(id);
				@Override
				public String getID() {
					return id;
				}
				@Override
				public String getContent() {
					try {
						return new String(ref.getContent(),UTF_8);
					} catch (IOException e) {
						log.error("Could not read content of " +id,e);
						return null;
					}
				}
				@Override
				public boolean createWithContent(String content) {
					try{
						ref.createWithContent(content.getBytes(UTF_8));
						return true;// TODO do real check here
					} catch (IOException e) {
						log.error("Could not read content of " +id,e);
						return false;
					}
				}
				@Override
				public void setContent(String content) {
					try{
						ref.setContent(content.getBytes(UTF_8));
					} catch (IOException e) {
						log.error("Could not write content of " +id,e);
					}
				}
				@Override
				public boolean rename(String newID) {
					return ref.rename(newID);
				}
				@Override
				public boolean delete(String delete) {
					return ref.delete();
				}
			};
		}
	}
	
	HashMap<String,MemoryStore> memStores=new HashMap<String,MemoryStore>();
	public class MemoryStore extends TextStore{
		ConcurrentHashMap<String,String> map=new ConcurrentHashMap<String,String>();
		@Override
		public Collection<String> list() {
			return map.keySet();
		}
		@Override
		public boolean has(String id) {
			return map.containsKey(id);
		}
		@Override
		public TextRef item(String id) {
			return new TextRef(){
				@Override
				public String getID() {
					return id;
				}
				@Override
				public String getContent() {
					return map.get(id);
				}
				@Override
				public boolean createWithContent(String content) {
					if(map.containsKey(id)) return false;
					Object o = map.putIfAbsent(id, content);
					return o==null;
				}
				@Override
				public void setContent(String content) {
					map.put(id,content);
				}
				@Override
				public boolean rename(String newID) {
					String v=map.remove(id);
					if(v!=null){
						map.put(newID, v);
						return true;
					}else{
						return false;
					}
				}
				@Override
				public boolean delete(String delete) {
					return map.remove(id)!=null;
				}
			};
		}
	}
	static String SCAN_POINTER_START = redis.clients.jedis.ScanParams.SCAN_POINTER_START; 
	
	public class FileStore extends TextStore{
		public FileStore(File folder){
			this.folder=folder;
			if(!folder.exists()) folder.mkdirs();
		}
		File folder=null;
		@Override
		public Collection<String> list() {
			LinkedList<String> out=new LinkedList<String>();
			for(File f : folder.listFiles()) out.add(f.getName());
			return out;
		}
		@Override
		public boolean has(String id) {
			return new File(folder,id).exists();
		}
		@Override
		public TextRef item(final String id) {
			return new TextRef() {
				File file=new File(folder,id);
				@Override
				public String getID() {
					return id;
				}
				@Override
				public String getContent() {
					try {
						return new String(Files.readAllBytes(Paths.get(file.getAbsolutePath())),UTF_8);
					} catch (IOException e) {
						e.printStackTrace();
						return null;
					}
				}
				@Override
				public boolean createWithContent(String content) {
					if(file.exists()) return false;
					setContent(content);
					return true;
				}
				@Override
				public void setContent(String content) {
					try (FileWriter fw=new FileWriter(file)){
						fw.write(content);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				@Override
				public boolean rename(String newID) {
					return file.renameTo(new File(folder,newID));
				}
				@Override
				public boolean delete(String delete) {
					return file.delete();
				}
			};
		}
	}
}
