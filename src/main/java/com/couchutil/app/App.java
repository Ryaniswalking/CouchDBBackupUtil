package com.couchutil.app;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.json.JSONArray;
import org.json.JSONObject;


public class App {

    private static final String db = "ech";
    private static final String fileOutputPath = "C:\\Users\\Rwalker\\Documents\\CouchDBBackups\\" +db+ "\\";
    
    private static final String baseUrl = "http://brdvwapp001:5984/";
    private static String endpoint = baseUrl + db;
   
    public static void main(String[] args) throws Exception {
        String encodedUrl = endpoint + "/_design_docs";

        JSONObject test = callCouchDb(encodedUrl);

        JSONArray array = test.getJSONArray("rows");

        Stack<String> designDocStack = new Stack<>();
        List<String> designDocList = new ArrayList<>();
        
        //create stack of all design docs
        for (int i = 0; i < array.length(); i++) {
            String couchName = array.getJSONObject(i).getString("id");
            designDocStack.add(couchName.substring(couchName.lastIndexOf("/") + 1));
            designDocList.add(couchName);
        }
        
        //design doc -> view names
        Map<String, Set<String>> designDocMap = new HashMap<>();
        
        for(String doc : designDocList){
            String url = endpoint + "/" + encodeUrl(doc);
            JSONObject viewName = callCouchDb(url);
            Set<String> viewNameKeys = viewName.getJSONObject("views").keySet();
            designDocMap.put(doc.substring(doc.lastIndexOf("/") + 1), viewNameKeys);
        }

        //design doc -> view name -> doc name -> doc ID
        Map<String, Map<String, Map<String, String>>> couchMap = new HashMap<>();
        Map<String, Map<String, String>> viewMap = new HashMap<>();
        Map<String,String> docIdMap;
        for(String key : designDocMap.keySet()){
            Set<String> viewNamesSet = designDocMap.get(key);
            viewMap = new HashMap<>();
            for(String viewName : viewNamesSet){
                String url = endpoint + "/_design/" + encodeUrl(key) + "/_view/" + encodeUrl(viewName);
                JSONObject viewDocs = callCouchDb(url);
                JSONArray arrayDocs = viewDocs.getJSONArray("rows");
                docIdMap = new HashMap<>();
                for(int i =0; i<arrayDocs.length(); i++){
                    String docName = arrayDocs.getJSONObject(i).getString("key");
                    String docId = arrayDocs.getJSONObject(i).getString("id");
                    String docUrl = endpoint + "/" + docId;
                    
                    String doc = getCouchAsString(docUrl);
                    docIdMap.put(docName, doc);
                }
                viewMap.put(viewName, docIdMap);
            }
            couchMap.put(key, viewMap);
        }

        String date = getDateNow();

        while(!designDocStack.isEmpty()){
            String designDoc = designDocStack.pop();    

            Map<String, Map<String, String>> views = couchMap.get(designDoc);
            for(String viewName : views.keySet()){
                Map<String, String> docMap = views.get(viewName);
                for(String docName : docMap.keySet()){
                    String jsonObject = docMap.get(docName);
                    Files.createDirectories(Paths.get(fileOutputPath + date + "\\" +  designDoc + "\\" + viewName + "\\"));
                    String filePath = fileOutputPath + date + "\\" + designDoc + "\\" + viewName + "\\" + removeIllegalChars(docName) + ".json" ;
                    FileWriter file = new FileWriter(filePath);
                    file.write(jsonObject);
                    file.close();
                }
            }
        }
        
    }

    public static JSONObject callCouchDb(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        con.setRequestProperty("Content-Type", "application/json");

        con.setRequestMethod("GET");
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer content = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();
        con.disconnect();

        return new JSONObject(content.toString());
    }

    private static String getDateNow() {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd"); 
		LocalDateTime now = LocalDateTime.now();  
		return  dtf.format((now));
	}

    public static String getCouchAsString(String urlStr) throws Exception{
        URL url = new URL(urlStr);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        con.setRequestProperty("Content-Type", "application/json");

        con.setRequestMethod("GET");
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer content = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();
        con.disconnect();

        return content.toString();
    }

    public static String encodeUrl(String url){
        try {
            return URLEncoder.encode(url, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            return "";
        }
    }

    public static String removeIllegalChars(String fileName){
        String invalidCharRemoved = fileName.replaceAll("[\\\\/:*?\"<>|]", "");
        return invalidCharRemoved;
    }
}
