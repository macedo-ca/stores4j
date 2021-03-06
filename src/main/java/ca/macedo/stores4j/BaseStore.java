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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

import ca.macedo.stores4j.BinStores.BinStore;

public abstract class BaseStore {
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
		baseFilter=new Filter(prefix,suffix0);
	}
	public String getSuffix() {
		return suffix0;
	}
	public void setSuffix(String suffix) {
		this.suffix0 = suffix;
		baseFilter=new Filter(prefix,suffix0);
	}
	String prefix=null;
	String suffix0=null;
	
	protected Filter baseFilter=new Filter();
	public abstract Collection<String> filter(String filter);
	public abstract Collection<String> list();

	public Filter prepareFilter(String storeTypePrefix, String filter){
		return prepareFilter(storeTypePrefix, filter, prefix, suffix0);
	}
	public static Filter prepareFilter(String storeTypePrefix, String filter, String prefix, String suffix0){
		Filter f=new Filter();
		int staridx=filter.indexOf('*');
		if(staridx>-1){
			f.filterPrefix=(storeTypePrefix + (prefix!=null?prefix:"") + filter.substring(0, staridx)).toLowerCase();
			if(!filter.endsWith("*")){
				f.filterSuffix = ((suffix0!=null?suffix0:"") + filter.substring(staridx+1)).toLowerCase();
			}else{
				f.filterSuffix = (suffix0!=null?suffix0.toLowerCase():null);
			}
		}else{
			f.filterPrefix=(storeTypePrefix + (prefix!=null?prefix:"")).toLowerCase();
		}
		return f;
	}
	public class MetaData{
		BaseStore store=null;
		String id=null;
		Long lastModified=null;
		String etag=null;
		Long length=null;
		HashMap<String,String> other=null;
		
		BinStore binStore(){
			return (BinStore)store;
		}

		public void setStore(BaseStore store) {
			this.store = store;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Long getLastModified() {
			return lastModified;
		}

		public void setLastModified(Long lastModified) {
			this.lastModified = lastModified;
		}

		public String getEtag() {
			return etag;
		}

		public void setEtag(String etag) {
			this.etag = etag;
		}

		public Long getLength() {
			return length;
		}

		public void setLength(Long length) {
			this.length = length;
		}

		public HashMap<String, String> getOther() {
			return other;
		}

		public void setOther(HashMap<String, String> other) {
			this.other = other;
		}
	}
	public static class Proxy{
		public Proxy(){}
		public Proxy(String settings){
			int idx=settings.indexOf('@');
			String hostport=null;
			if(idx>-1){
				String up=settings.substring(0, idx);
				hostport=settings.substring(idx+1);
				idx=up.indexOf(':');
				user=up.substring(0,idx);
				pwd=up.substring(idx+1);
				user=user.replace(';', '\\');
			}else{
				hostport=settings;
			}
			idx=hostport.indexOf(':');
			if(idx>-1){
				host=hostport.substring(0, idx);
				port=hostport.substring(idx+1);
			}else{
				host=hostport;
			}
		}
		public void setUser(String user) {
			this.user = user;
		}
		public void setPwd(String pwd) {
			this.pwd = pwd;
		}
		public void setHost(String host) {
			this.host = host;
		}
		public void setPort(String port) {
			this.port = port;
		}
		String user=null;
		String pwd=null;
		String host=null;
		String port="8080";
	}
	public static class Filter{
		protected boolean all(){
			return filterPrefix==null && filterSuffix==null;
		}
		Filter(){}
		Filter(String pre, String suf){
			filterPrefix=pre;
			filterPrefixLow=(pre!=null?pre.toLowerCase():null);
			filterSuffix=suf;
		}
		String filterPrefix=null;
		String filterPrefixLow=null;
		String filterSuffix=null;
		protected Collection<String> filter(Collection<String> list){
			Collection<String> out=new LinkedList<String>();
			for(String s : list){
				String sLowCase=s.toLowerCase();
				if((filterPrefix==null || (sLowCase.startsWith(filterPrefix))) && (filterSuffix==null || (sLowCase.endsWith(filterSuffix)))) out.add(s);
			}
			return out;
		}
		protected boolean match(String entry){
			String sLowCase=entry.toLowerCase();
			return ((filterPrefixLow==null || (sLowCase.startsWith(filterPrefixLow))) && (filterSuffix==null || (sLowCase.endsWith(filterSuffix))));
		}
		public String wildCardFilter() {
			return (filterPrefix!=null?filterPrefix:"") + "*" + (filterSuffix!=null?filterSuffix:"");
		}
	}
}