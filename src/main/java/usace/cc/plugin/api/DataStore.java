package usace.cc.plugin.api;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties
public class DataStore {

    public static class DataStoreException extends Exception{

        public DataStoreException(Exception ex){
            super(ex);
        }

        public DataStoreException(String msg){
            super(msg);
        }
    }
    
    @JsonProperty
    private String name;

    @JsonProperty
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String id;

    //read as Map<String, Object> so that we don't have to write a custom json deserializser in Jackson
    //convert to PayloadAttributes in the accessor method.
    @JsonProperty("params")
    @JsonIgnoreProperties(ignoreUnknown = true)
    private Map<String, Object> parameters;

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


    //@TODO discuss this with will.  this is kind of fragile
    @SuppressWarnings("unchecked")
    public <T> T session(Class<T> sessionType){
        if (session instanceof ConnectionDataStore connectedSession){
            if(sessionType.isInstance(session)){
                return (T)session;
            } else{
                return (T)connectedSession.rawSession();
            }
        }
        return (T)session;
    }

    public Object getSession(){
        return session;
    }

    public Object getRawSession(){
        if (session instanceof ConnectionDataStore connectedSession){
            return connectedSession.rawSession();
        }
        return session;
    }

    public void setSession(Object session){
        this.session = session;
    }

}