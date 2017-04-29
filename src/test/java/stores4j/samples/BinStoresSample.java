package stores4j.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

import ca.macedo.stores4j.BinStores;
import ca.macedo.stores4j.BinStores.BinStore;

public class BinStoresSample {
	public static void main(String[] ar) throws IOException{
		// 0. Create store instance
		BinStores binStores=new BinStores();
		BinStore fileStore = binStores.getStore("file://C:/temp/");
		
		// 1. List / Filter - Get a listing / Filtered listing of entry IDs
		for(String id : fileStore.list()) System.out.println(id);
		for(String id : fileStore.filter("*.txt")) System.out.println(id);
		
		// 2. Visit - visit all / filtered-list of entries
		fileStore.forEach( entry -> {
			try {
				System.out.println(entry.getID() +", size " + entry.getContent().length + " bytes");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} );
		fileStore.forEachFiltered( entry -> {
			try {
				System.out.println(entry.getID() +", size " + entry.getContent().length + " bytes");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} , "*.txt");
		
		// 3. Read / Write bytes
		byte[] pictureData=fileStore.item("test.jpg").getContent();
		fileStore.item("test_jpg_base64_encoded.txt").setContent(Base64.getEncoder().encode(pictureData));
		
		// 4. Read / Write streams
		try(InputStream is=fileStore.item("test.jpg").getContentStream()){
			// The second optional parameter is length, if known, which can optimize large data transfers
			fileStore.item("test_copy.jpg").setContentStream(is, null); 
		}
		
		// 5. Bulk operations - Copy / Replicate
		BinStore fileStoreCopy = binStores.getStore("file://C:/temp/copyOfTemp/");
		fileStore.copyTo(fileStoreCopy);
		// The difference between copy and replica is that replica removes any entries not in the original
		BinStore fileStoreReplica = binStores.getStore("file://C:/temp/replicaOfTemp/");
		fileStore.replicateTo(fileStoreReplica);
	}
}
