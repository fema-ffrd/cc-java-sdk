package usace.cc.plugin.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Action extends IOManager{

    @JsonProperty
    private String type;
    
    @JsonProperty("description")
    private String desc;
    
    public String getType(){
        return type;
    }

    public String getDescription(){
        return desc;
    }

}
