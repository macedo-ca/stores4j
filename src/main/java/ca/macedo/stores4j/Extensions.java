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

public class Extensions<T,C> {
	BaseExtention<T,C> extensions=null;
	public synchronized void register(BaseExtention<T,C> ext){
		if(extensions==null){
			extensions=ext;
			return;
		}
		extensions.add(ext);
	}
	public T tryCreate(C context, String uri){
		if(extensions!=null){
			return extensions.tryCreate(context, uri);
		}
		return null;
	}
	public static abstract class BaseExtention<T,C>{
		public abstract boolean isMine(C context, String uri);
		public abstract T get(C context,String uri);
		
		BaseExtention<T,C> next=null;
		public void add(BaseExtention<T,C> newOne){
			if(newOne == this) return;
			if(next!=null) next.add(newOne);
			next=newOne;
		}
		T tryCreate(C context,String uri){
			if(isMine(context,uri)){
				return get(context,uri);
			}
			if(next!=null) return next.tryCreate(context,uri);
			return null;
		}
	}

}
