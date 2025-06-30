package usace.cc.plugin.api;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an executable action with a specific type and description.
 * <p>
 * This class extends {@link IOManager}, potentially inheriting I/O-related behavior.
 */

public class Action extends IOManager{

    @JsonProperty
    private String name;

    @JsonProperty
    private String type;
    
    @JsonProperty("description")
    private String desc;
    
    public String getName(){
        return name;
    }

    public String getType(){
        return type;
    }

    public String getDescription(){
        return desc;
    }

}
