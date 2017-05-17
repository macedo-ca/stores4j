package ca.macedo.stores4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Iterator;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.io.IOUtils;

import ca.macedo.stores4j.BaseStore.MetaData;
import ca.macedo.stores4j.BinStores.BinStore;
import ca.macedo.stores4j.BinStores.BinStore.BinRef;
import ca.macedo.stores4j.TextStores.TextStore;
import ca.macedo.stores4j.TextStores.TextStore.TextRef;

public class StoresProcess {
	private static Charset UTF8 = Charset.forName("UTF-8");
	
	Session sess=new Session();
	Step first=null;
	BinStore toBStore=null;
	
	StoresProcess(Source src){
		first=new Step(src);
	}
	
	public static void main(String[] ar){
		BinStores str=new BinStores();
		StoresProcess
			.from(str.getStore("file:C:/tmp/csu_dump/"))
			.log(System.out)
			.ensureSuffix(".json")
			.script("var o = JSON.parse(source);o.b='yepp'; JSON.stringify(o);")
			.transformJson("{newField:from.a, anotherField: from.b}")
			.to(str.getStore("file:C:/tmp/csu_dump22/"))
			.run()
		; 
	}
	
	// Source filters
	public StoresProcess onlyFilter(String filter) {
		first.source().filter(filter);
		return this;
	}
	public StoresProcess onlyID(String id) {
		first.source().id(id);
		return this;
	}
	
	// Processing steps
	public StoresProcess setSuffix(String suffix) {
		first.add(new Step(){ @Override void process(Session sess) {
			String sfx=suffix;
			if(!sfx.startsWith(".")) sfx="."+sfx;
			String id=sess.current.getId();
			sess.current.setId(id+sfx);
			sess.log("'" + id+"' suffixed to '"+id+sfx+"'");
		}});
		return this;
	}
	public StoresProcess ensureSuffix(String suffix) {
		first.add(new Step(){ @Override void process(Session sess) {
			String sfx=suffix;
			if(!sfx.startsWith(".")) sfx="."+sfx;
			String id=sess.current.getId();
			if(!id.toLowerCase().endsWith(sfx.toLowerCase())){
				sess.current.setId(id+sfx);
				sess.log("'" + id+"' suffixed to '"+id+sfx+"'");
			}
		}});
		return this;
	}
	public StoresProcess script(String script) {
		ScriptEngine eng=new ScriptEngineManager().getEngineByName("javascript");
		try {
			Compilable comp=(Compilable)eng;
			final CompiledScript sc=comp.compile(script);
			first.add(new Step(){ @Override void process(Session sess) {
				Bindings b=eng.createBindings();
				b.put("id", sess.current.getId());
				b.put("metaData", sess.current);
				b.put("source", sess.source);
				b.put("log", log);
				try {
					Object o =sc.eval(b);
					if(o!=null){
						sess.source.setContent(o);
					}
					sess.log("Ran script for '"+sess.source.id()+"'");
				} catch (ScriptException e) {
					sess.log("Script run failed: "+e.getMessage());
				}
			}});
		} catch (ScriptException e) {
			e.printStackTrace();
		}
		return this;
	}
	public StoresProcess transformJson(String newJSobject) {
		ScriptEngine eng=new ScriptEngineManager().getEngineByName("javascript");
		try {
			Compilable comp=(Compilable)eng;
			final CompiledScript sc=comp.compile("var from=JSON.parse(source); var out="+newJSobject+"; JSON.stringify(out);");
			first.add(new Step(){ @Override void process(Session sess) {
				Bindings b=eng.createBindings();
				b.put("source", sess.source);
				try {
					Object o =sc.eval(b);
					if(o!=null){
						sess.source.setContent(o);
					}
					sess.log("Transformed '"+sess.source.id()+"' JSON");
				} catch (ScriptException e) {
					sess.log("Script run failed: "+e.getMessage());
				}
			}});
		} catch (ScriptException e) {
			e.printStackTrace();
		}
		return this;
	}
	
	// Targets
	public StoresProcess log(PrintStream os){
		setLog(os);
		return this;
	}
	public StoresProcess toLog(){
		sess.toLog=true;
		return this;
	}
	public StoresProcess to(BinStore store){
		toBStore=store;
		return this;
	}
	
	public StoresProcess run(){
		sess.runAll();
		log.flush();
		return this;
	}
	
	
	StringWriter defLog=new StringWriter();
	PrintWriter log=new PrintWriter(defLog);
	public void setLog(OutputStream os){
		log = new PrintWriter(new OutputStreamWriter(os));
	}
	
	//////////////////////
	public class Session{
		boolean toLog=false;;
		MetaData current=null;
		Source source=null;
		
		public void runAll(){
			source=first.source();
			while(hasNext()){
				first.run(this);
				result(current,source);
				if(toLog){
					log.println(source.string());
				}
			}
		}
		public void log(String s){
			log.println(s);
		}
		public void result(MetaData d, Source s){
			InputStream is=null;
			String str=null;
			Long len=null;
			if(source.newContent!=null){
				str=source.newContent;
			}else{
				try {
					is=s.isBin() ? s.getStream() : null;
					len = d.getLastModified();
				} catch (IOException e) {
					e.printStackTrace();
				}
				str=s.isText() ? s.getSourceString() : null;
			}
			if(toBStore!=null){
				if(is==null || str!=null){
					byte[] bts=str.getBytes(UTF8);
					is = new ByteArrayInputStream(bts);
					len = new Long(bts.length);
				}
				try {
					toBStore.item(d.getId()).setContentStream(is,len, d.getLastModified());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		boolean hasNext(){
			boolean out =source.hasNext();
			if(out){
				current = source.metaData();
			}else{
				current=null;
			}
			return out;
		}

	}
	private class Step{
		Step(){}
		Step(Source src){
			this.src=src;
		}
		Source src=null;
		Step previous=null;
		Step next=null;
		Source source(){
			return src!=null ? src : (previous!=null?previous.source():null);
		}
		
		void add(Step st){
			if(next!=null){
				next.add(st);
			}else{
				next=st;
			}
		}
		void run(Session sess){
			process(sess);
			if(next!=null) next.run(sess);
		}
		void process(Session sess){}
	}
	
	// Types of sources
	private abstract static class Source{
		String filter=null;
		String newContent=null;
		void filter(String filter){
			this.filter=filter;
		}
		public void set(String o) {
			newContent=o;
		}
		public void setContent(Object o) {
			newContent=o.toString();
		}
		String id=null;
		void id(String id){
			this.id=id;
		}
		String id(){
			return id;
		}
		boolean hasNext(){
			return false;
		}
		boolean isBin(){
			return false;
		}
		boolean isText(){
			return newContent!=null ? true : false;
		}
		public abstract MetaData metaData();
		public InputStream getStream() throws IOException{
			return null;
		}
		public String getSourceString(){
			return null;
		}
		public String string(){
			return newContent!=null ? newContent : getSourceString();
		}
		public String toString(){
			return string();
		}
	}
	static abstract class StoreSource extends Source{
		StoreSource(BaseStore st){
			store=st;
		}
		Iterator<String> it=null;
		String currid=null;
		BaseStore store=null;
		
		boolean idDone=false;
		String id(){
			return id!=null ? id : currid;
		}
		@Override
		boolean hasNext() {
			if(id!=null){
				if(idDone){
					clearCurrent();
					return false;
				}
				idDone=true;
				setCurrent(id);
				return true;
			}
			if(it==null){
				it = (filter!=null ? store.filter(filter) : store.list()).iterator();
			}
			boolean out=it.hasNext();
			if(out){
				setCurrent(currid=it.next());
			}else{
				clearCurrent();
				currid=null;
			}
			return out;
		}
		public abstract void setCurrent(String id);
		public abstract void clearCurrent();
		
		public abstract MetaData metaData() ;
	}
	
	
	public static StoresProcess from(final BinStore store){
		return new StoresProcess(new StoreSource(store){
			BinRef ref=null;
			
			@Override
			boolean isBin() { return true; }
			
			@Override
			public void setCurrent(String id) {
				ref=((BinStore)store).item(id);
			}
			@Override
			public void clearCurrent() {
				ref=null;
			}
			
			@Override
			public String getSourceString() {
				ByteArrayOutputStream bos=new ByteArrayOutputStream();
				try {
					IOUtils.copy(getStream(), bos);
					return new String(bos.toByteArray(),"UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
			
			@Override
			public InputStream getStream() throws IOException {
				return ref.getContentStream();
			}
			@Override
			public MetaData metaData() {
				return ref.getMetaData();
			}
		});
	}
	public static StoresProcess from(TextStore store){
		return new StoresProcess(new StoreSource(store){
			TextRef ref=null;
			
			@Override
			boolean isText() { return true; }
			@Override
			public void setCurrent(String id) {
				ref=((TextStore)store).item(id);
			}
			@Override
			public void clearCurrent() {
				ref=null;
			}
			@Override
			public String getSourceString() {
				return ref.getContent();
			}
			@Override
			public MetaData metaData() {
				return ref.getMetaData();
			}
		});
	}

}
