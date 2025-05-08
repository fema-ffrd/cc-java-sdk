package usace.cc.plugin.api;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PayloadAttributes {

    public PayloadAttributes(Map<String,Object> attrs){
        this.attributes=attrs;
    }

    @JsonProperty
    private Map<String,Object> attributes;
    
    public Map<String,Object> getAttributes(){
        return attributes;
    }

    public Set<Entry<String,Object>> entrySet(){
        return attributes.entrySet();
    }

    public Set<String> keySet(){
        return attributes.keySet();
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(String name) throws IllegalArgumentException{
        Object val = attributes.get(name);
        if (val==null){
            return Optional.empty();
        }else{
            try {
                T tval =  (T) val;
                return Optional.of(tval);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Invalid type cast.", e);
            }  
        }
    }

    //@TODO delete if uneccesary
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getAlt1(String name, Class<T> clazz) throws IllegalArgumentException{
        Object val = attributes.get(name);
        if (val==null){
            return Optional.empty();
        }else{
            if (clazz.isInstance(val)){
                return Optional.of((T)val);
            }
            throw new IllegalArgumentException("Incorrect type");   
        }
    }
}