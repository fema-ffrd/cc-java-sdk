package usace.cc.plugin.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Config {

    @JsonProperty
    public AWSConfig[] aws_configs;

}