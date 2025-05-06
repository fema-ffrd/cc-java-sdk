package usace.cc.plugin.api;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties
public class DataStore {
    
    @JsonProperty
    private String name;

    @JsonProperty
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String id;

    @JsonProperty("params")
    @JsonIgnoreProperties(ignoreUnknown = true)
    private Map<String, Object> parameters;
    //private PayloadAttributes parameters;

    @JsonProperty("store_type")
    private StoreType storeType;

    @JsonProperty("profile")
    private String dsProfile;

    //@TODO can session be a narrower type or a generic?
    private Object session;
    
    public String getName(){
        return name;
    }
    public String getId(){
        return id;
    }
    public StoreType getStoreType(){
        return storeType;
    }
    public PayloadAttributes getParameters(){
        return new PayloadAttributes(parameters);
    }
    public String getDsProfile(){
        return dsProfile;
    }
    public Object getSession(){
        return session;
    }
    public void setSession(Object session){
        this.session = session;
    }

}