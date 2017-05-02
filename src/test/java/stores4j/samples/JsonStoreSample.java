package stores4j.samples;

import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import ca.macedo.stores4j.TextStores;
import ca.macedo.stores4j.TextStores.JsonStore;

public class JsonStoreSample {
	public static void main(String[] ar) throws IOException{
		// 0. Create store instance
		TextStores txtStores = new TextStores();
		JsonStore  jsonStore = txtStores.getJsonStore("file://C:/temp/json/");
		
		// 1. List / Filter - Get a listing / Filtered listing of entry IDs
		JSONArray list = jsonStore.list();
		System.out.println(list.toJSONString());
		JSONArray filtered = jsonStore.filter("*.json");
		System.out.println(filtered.toJSONString());
		
		// 2. Read / Write JSON documents
		JSONObject textfileData=jsonStore.item("test.json").getContent();
		jsonStore.item("test_copy.json").setContent(textfileData.get("content"));
	}
}