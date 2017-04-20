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

import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.JsonElement;

public class StoresJSONBuilder {
	public static void main(String[] ar){
		array()
			.set("a", 1, "b", 2)
			.next()
			.set("a", 1, "b", 2)
			.root().println(System.out);
		
		object()
			.set("name","John", "lastname","Doe", "age","48")
			.sub("address", (e)-> e.set("street", "John St", "streetNum", 55))
			.root().println(System.out);
	}
	
	static StoresJSONBuilder singleton=new StoresJSONBuilder();
	
	public static JSONRoot from(String s){
		return singleton.newFromString(s);
	}
	public static JSONArrRoot array(){
		return singleton.newArray();
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static JSONArrRoot array(Consumer saveTo){
		return singleton.newArray().saveTo(saveTo);
	}
	public static JSONRoot object(){
		return singleton.newObject();
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static JSONRoot object(Consumer saveTo){
		return singleton.newObject().saveTo(saveTo);
	}
	
	boolean deepConvertDefault=false;
	
	public boolean isDeepConvertDefault() {
		return deepConvertDefault;
	}

	public void setDeepConvertDefault(boolean deepConvertDefault) {
		this.deepConvertDefault = deepConvertDefault;
	}

	public StoresJSONBuilder(){}
	public JSONRoot newFrom(Object o){
		return newFrom(o,deepConvertDefault);
	}
	@SuppressWarnings("rawtypes")
	public JSONRoot newFrom(Object o, boolean deepConvert){
		if(o instanceof String){
			return newFromString((String)o);
		}
		if(o instanceof JSONObject){
			return newFromObject((JSONObject)o);
		}
		if(o instanceof Collection<?>){
			return newFromArray((Collection<?>)o);
		}
		if(o instanceof JSONArray){
			return newFromArray((JSONArray)o);
		}
		if(o instanceof Map){
			return newFromMap((Map)o,deepConvert);
		}
		if(o instanceof JsonElement){
			return newFromString(((JsonElement)o).toString());
		}
		return null;
	}
	public JSONArrRoot newFromArray(JSONArray arr){
		return new JSONArrRoot(arr);
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public JSONArrRoot newFromArray(Collection arr){
		JSONArrRoot ar=newArray();
		for(Object o : arr){
			if(arr instanceof Map){
				ar.set((Map)o);
				ar.next();
			}else if(o instanceof String){
				ar.set((String)o);
				ar.next();
			}
		}
		return ar;
	}
	public JSONRoot newFromObject(JSONObject obj){
		return new JSONRoot(obj);
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public JSONRoot newFromMap(Map o, boolean deepConvert){
		return object().set((Map<String,Object>)o,deepConvertDefault).root();
	}
	public JSONRoot newFromString(String json){
		try {
			Object o=new JSONParser().parse(json);
			if(o instanceof JSONObject){
				return new JSONRoot((JSONObject)o);
			}else{
				return new JSONArrRoot((JSONArray)o);
			}
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	public JSONArrRoot newArray(){
		return new JSONArrRoot();
	}
	public JSONRoot newObject(){
		return new JSONRoot();
	}
	
	public String convert2jsonString(Object o){
		if(o==null) return null;
		if(o instanceof JSONObject){
			o=((JSONObject)o).toJSONString();
		}else if(o instanceof String){
			// noop
		}else{
			o=newFrom(o,deepConvertDefault).toJSON();
		}
		return (String)o;
	}
	@SuppressWarnings("rawtypes")
	public JSONArrRoot convert2arr(Object o){
		if(o==null) return null;
		if(o instanceof Map) o = ((Map)o).values();
		JSONEntry en=newFrom(o,true);
		if(en instanceof JSONArrRoot){
			return (JSONArrRoot)en;
		}
		return null;
	}
	
	public interface JSONEntryVisitor{
		public JSONEntry visit(JSONEntry e);
	}
	public class JSONEntry{
		JSONEntry parent=null;
		JSONObject curr=null;
		
		public JSONEntry sub(String name){
			return addSub(name, new JSONEntry());
		}
		public JSONEntry sub(String name, Map<String,Object> entr){
			return addSub(name, new JSONEntry()).set(entr);
		}
		public JSONEntry sub(String name, String json){
			return addSub(name, new JSONEntry()).set(json);
		}
		public JSONEntry sub(String name,JSONEntryVisitor v){
			JSONEntry e=sub(name);
			v.visit(e);
			return e;
		}
		@SuppressWarnings("unchecked")
		private JSONEntry addSub(String name, JSONEntry e){
			e.parent=this;
			e.curr=new JSONObject();
			curr.put(name, e.curr);
			return e;
		}
		/**
		 * Parse JSON and add as a sub-object. <i>This returns the CURRENT object, not the sub</i>
		 * @param attr
		 * @param json
		 * @return current node, not the new entry
		 */
		@SuppressWarnings("unchecked")
		public JSONEntry setJson(String attr, String json){
			try {
				curr.put(attr, new JSONParser().parse(json));
				return this;
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}
		@SuppressWarnings("unchecked")
		private void set0(String attr, Object val){
			curr.put(attr, val);
		}
		public JSONEntry set(String attr, Object val){
			set0(attr,val);
			return this;
		}
		public JSONEntry set(String attr, Object val, String attr2, Object val2){
			set0(attr,val);
			set0(attr2,val2);
			return this;
		}
		public JSONEntry set(String attr, Object val, String attr2, Object val2, String attr3, Object val3){
			set0(attr,val);
			set0(attr2,val2);
			set0(attr3,val3);
			return this;
		}
		public JSONEntry set(String attr, Object val, String attr2, Object val2, String attr3, Object val3, String attr4, Object val4){
			set0(attr,val);
			set0(attr2,val2);
			set0(attr3,val3);
			set0(attr4,val4);
			return this;
		}
		public JSONEntry set(String attr, Object val, String attr2, Object val2, String attr3, Object val3, String attr4, Object val4, String attr5, Object val5){
			set0(attr,val);
			set0(attr2,val2);
			set0(attr3,val3);
			set0(attr4,val4);
			set0(attr5,val5);
			return this;
		}
		/**
		 * Parse JSON and add all attributes to current node
		 * @param json
		 * @return
		 */
		@SuppressWarnings("unchecked")
		public JSONEntry set(String json){
			try {
				JSONObject o=(JSONObject)new JSONParser().parse(json);
				set(o);
				return this;
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}
		/**
		 * Copy all attributes to current node
		 * @param json
		 * @return
		 */
		public JSONEntry set(Map<String,Object> vals){
			for(Map.Entry<String,Object> e : vals.entrySet()){
				set0(e.getKey(), e.getValue());
			}
			return this;
		}
		/**
		 * Copy all attributes to current node
		 * @param json
		 * @return
		 */
		@SuppressWarnings({ "rawtypes", "unused" })
		public JSONEntry set(Map<String,Object> vals,boolean deepConvert){
			for(Map.Entry<String,Object> e : vals.entrySet()){
				Object o=e.getValue();
				if(deepConvert && o instanceof Map){
					JSONRoot innerObj=newFromMap((Map)o, true);
				}
				set0(e.getKey(), o);
			}
			return this;
		}
		
		/**
		 * Return parent node
		 * @return
		 */
		public JSONEntry endSub(){
			return parent;
		}
		/**
		 * Cover for array root
		 * @return
		 */
		public JSONEntry next(){
			// noop
			return this;
		}
		/**
		 * Get the root entry
		 * @return
		 */
		public JSONRoot root(){
			JSONEntry e=this;
			while(e.parent!=null){
				e=e.parent;
			}
			return (JSONRoot)e;
		}
		public JSONRoot save(){
			return root().save();
		}
		public String toJSON(){
			return curr.toJSONString();
		}
		public Object toJSONSimple(){
			return curr;
		}
		public JSONObject toJSONObject(){
			return curr;
		}
		@SuppressWarnings("unchecked")
		public JSONArray toJSONArray(){
			JSONArray ar=new JSONArray();
			ar.add(curr);
			return ar;
		}
		public JSONEntry println(PrintStream out){
			out.println(toJSON());
			return this;
		}
	}
	
	public class JSONRoot extends JSONEntry{
		Consumer<Object> saveTo=null;
		public JSONRoot saveTo(Consumer<Object> saveTo){
			this.saveTo=saveTo;
			return this;
		}
		public JSONRoot save(){
			if(saveTo!=null) saveTo.accept(curr);
			return this;
		}
		public JSONRoot() {
			curr=new JSONObject();
		}
		public JSONRoot(JSONObject o) {
			curr=o;
		}
		JSONArray rootArr=null;
	}
	public interface ArrayItemsVisitor{
		public void visit(JSONEntry e, int i, int size);
	}
	public class JSONArrRoot extends JSONRoot{
		JSONArray rootArr=null;
		public JSONArrRoot saveTo(Consumer<Object> saveTo){
			this.saveTo=saveTo;
			return this;
		}
		public JSONRoot save(){
			if(saveTo!=null) saveTo.accept(rootArr);
			return this;
		}
		public int size(){
			return rootArr.size();
		}
		public JSONEntry get(int i){
			return get0(new JSONEntry(),i);
		}
		JSONEntry get0(JSONEntry e, int i){
			curr=(JSONObject)rootArr.get(i);
			e.curr=curr;
			e.parent=this;
			return e;
		}
		public JSONArrRoot forEach(ArrayItemsVisitor vis){
			JSONEntry e=new JSONEntry();
			int size=size();
			for(int i=0;i<size;i++){
				vis.visit(get0(e,i), i, size);
			}
			return this;
		}
		public String[] toStringArray(){
			JSONEntry e=new JSONEntry();
			int size=size();
			String[] out=new String[size];
			for(int i=0;i<size;i++){
				get0(e,i);
				out[i]=e.toJSON();
			}
			return out;
		}
		public JSONArrRoot(JSONArray ar){
			rootArr=ar;
			curr=ar.size()>0 && ar.get(0) instanceof JSONObject ? (JSONObject)ar.get(0) : null;
		}
		public JSONArrRoot(){
			rootArr=new JSONArray();
			curr=new JSONObject();
		}
		@SuppressWarnings("unchecked")
		@Override
		public JSONEntry next(){
			if(curr!=null) rootArr.add(curr);
			curr=new JSONObject();
			return this;
		}
		@Override
		public String toJSON(){
			return rootArr.toJSONString();
		}
		@Override
		public Object toJSONSimple(){
			return rootArr;
		}
		@Override
		public JSONArray toJSONArray(){
			return rootArr;
		}
	}
}
