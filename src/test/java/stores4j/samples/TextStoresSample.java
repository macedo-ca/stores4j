package stores4j.samples;

import java.io.IOException;
import ca.macedo.stores4j.TextStores;
import ca.macedo.stores4j.TextStores.TextStore;

public class TextStoresSample {
	public static void main(String[] ar) throws IOException{
		// 0. Create store instance
		TextStores binStores=new TextStores();
		TextStore fileStore = binStores.getStore("file://C:/temp/");
		
		// 1. List / Filter - Get a listing / Filtered listing of entry IDs
		for(String id : fileStore.list()) System.out.println(id);
		for(String id : fileStore.filter("*.txt")) System.out.println(id);
		
		// 2. Read / Write bytes
		String textfileData=fileStore.item("test.txt").getContent();
		fileStore.item("test_copy.txt").setContent(textfileData.toUpperCase());
		
		// 3. Bulk operation - Copy
		TextStore textStoreCopy = binStores.getStore("file://C:/temp/copyOfTextTemp/");
		fileStore.copyTo(textStoreCopy,"*.txt");
	}
}