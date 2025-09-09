package usace.cc.plugin.api;

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataSource {

    @JsonProperty
    private String name;
    
    @JsonProperty
    private String id;
    
    @JsonProperty("store_name")
    private String storeName;
    
    @JsonProperty
    private Map<String,String> paths;
    
    @JsonProperty("data_paths")
    private Map<String,String> dataPaths;
    
    public String getId(){
        return id;
    }
    
    public String getName(){
        return name;
    }

    public void setName(String name){
        this.name=name;
    }
    
    public Map<String,String> getPaths(){
        return this.paths;
    }

    public String getPath(String pathkey){
        return this.paths.get(pathkey);
    }

    public void setPaths(Map<String,String> paths){
        this.paths=paths;
    }
    
    public Optional<Map<String,String>> getDataPaths(){
        return Optional.ofNullable(dataPaths);
    }

    public void setDataPaths(Map<String,String> datapaths){
        this.dataPaths=datapaths;
    }

    public String getStoreName(){
        return storeName;
    }
    
}