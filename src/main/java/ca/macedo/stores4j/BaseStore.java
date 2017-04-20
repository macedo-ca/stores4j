package ca.macedo.stores4j;

import java.util.Collection;
import java.util.LinkedList;

public class BaseStore {
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
	
	protected Filter prepareFilter(String storeTypePrefix, String filter){
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
	
	protected class Filter{
		protected boolean all(){
			return filterPrefix==null && filterSuffix==null;
		}
		Filter(){}
		Filter(String pre, String suf){
			filterPrefix=pre;
			filterSuffix=suf;
		}
		String filterPrefix=null;
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
			return ((filterPrefix==null || (sLowCase.startsWith(filterPrefix))) && (filterSuffix==null || (sLowCase.endsWith(filterSuffix))));
		}
		public String wildCardFilter() {
			return (filterPrefix!=null?filterPrefix:"") + "*" + (filterSuffix!=null?filterSuffix:"");
		}
	}
}