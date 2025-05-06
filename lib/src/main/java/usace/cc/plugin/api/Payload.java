package usace.cc.plugin.api;

import com.fasterxml.jackson.annotation.JsonProperty;


public class Payload extends IOManager{
    
    @JsonProperty
    private Action[] actions;
    
    public Action[] getActions(){
        return actions;
    }
    
}

