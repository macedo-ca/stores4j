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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class BinStoresWithS3 extends BinStores{
	@Override
	protected BinStore extendedGetStore(BinStores mainStores, String uri) {
		if(uri.toLowerCase().startsWith("s3:")){
			try{
				String accessKey=null;
				String secretKey=null;
				
				int start=uri.indexOf("://")>-1 ? 5 : 3;
				int atIdx=uri.indexOf('@');
				String keys= atIdx>-1 ? uri.substring(start, atIdx) : mainStores.defaultCredentials("s3");
				if(keys==null || keys.trim().length()==0) throw new RuntimeException("No s3 credentials provided");
				accessKey = keys.substring(0,keys.indexOf(':'));
				secretKey = keys.substring(keys.indexOf(':')+1);
				
				String[] parts=atIdx>-1 ? uri.substring(atIdx+1).split("/") : uri.substring(start).split("/");
				if(parts.length<2){
					s3log.error("Not a valid S3 url. Format should be s3://[accessKey]:[secretKey]@[region]/[bucket][/folder (OPTIONAL)]" + uri);
					return null;
				}
				String region=parts[0];
				String bucket=parts[1];
				String folder = parts.length>2 ? parts[2] : "";
				
				AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
				AmazonS3ClientBuilder bld = AmazonS3Client.builder()
						.withRegion(region)
						.withCredentials((new AWSStaticCredentialsProvider(credentials)));
				
				AmazonS3 s3Client = bld.build();
				
				if(!s3Client.doesBucketExist( bucket )){
					s3log.error("Bucket does not exist " + uri);
				}else{
					return new S3FileStore(s3Client,bucket,folder);
				}
			} catch (Throwable e) {
				s3log.error("Could not open S3 bucket " + uri,e);
			}
		}
		return null;
	}
	private static Logger s3log=LoggerFactory.getLogger(S3FileStore.class);
	public class S3FileStore extends BinStore{
		public S3FileStore(AmazonS3 s3Client, String bucket, String folder){
			this.s3Client=s3Client;
			this.bucket=bucket;
			if(folder.length()>0 && !folder.endsWith("/")) folder=folder+"/";
			this.folder=folder;
		}
		AmazonS3 s3Client=null;
		String bucket=null;
		String folder=null;
		
		@Override
		public BinStore folder(String name) {
			return new S3FileStore(s3Client,bucket,(folder.length()>0?folder:"")+name);
		}
		@Override
		public Collection<String> filter(String filter) {
			return list0(prepareFilter("", filter));
		}
		@Override
		public Collection<String> list() {
			return list0(baseFilter);
		}
		private Collection<String> list0(Filter f) {
			LinkedList<String> out=new LinkedList<String>();
			int fl=folder.length();
			for(S3ObjectSummary os : s3Client.listObjects(bucket, folder+(f.filterPrefix!=null?f.filterPrefix:"")).getObjectSummaries()){
				String k = os.getKey().substring(fl);
				if(f.match(k)) out.add(k);
			}
			return out;
		}
		@Override
		public boolean has(String id) {
			return s3Client.doesObjectExist(bucket, folder+id);
		}
		@Override
		public BinRef item(final String id) {
			return new BinRef() {
				String key=folder+id;
				@Override
				public String getID() {
					return id;
				}
				@Override
				public byte[] getContent()throws IOException {
					return IOUtils.toByteArray(getContentStream());
				}
				@Override
				public InputStream getContentStream() throws IOException {
					return s3Client.getObject(bucket, key).getObjectContent();
				}
				@Override
				public void copyTo(BinRef other) throws IOException {
					S3Object obj=s3Client.getObject(bucket, key);
					int len=(int)obj.getObjectMetadata().getContentLength();
					other.setContentStream(obj.getObjectContent(), len);
				}
				@Override
				public void setContent(byte[] content)throws IOException {
					setContentStream(new ByteArrayInputStream(content), content.length);
				}
				@Override
				public void setContentStream(InputStream st, Integer lengthIfKnown) throws IOException {
					ObjectMetadata md=new ObjectMetadata();
					if(lengthIfKnown!=null) md.setContentLength(lengthIfKnown);
					s3Client.putObject(bucket, key, st, md);
				}
				@Override
				public OutputStream setContentStream() throws IOException {
					return new ByteArrayOutputStream(){
						@Override
						public void close() throws IOException {
							setContent(this.toByteArray());
							super.close();
						}
					};
				}
				@Override
				public boolean delete() {
					s3Client.deleteObject(bucket, key);
					return true;
				}
				@Override
				public boolean rename(String newID) {
					s3Client.copyObject(bucket, key, bucket, newID);
					s3Client.deleteObject(bucket,key);
					return false;
				}
				@Override
				public boolean createWithContent(byte[] content) throws IOException {
					if(has(id)) return false;
					setContent(content);
					return true;
				}
			};
		}
	}
}
