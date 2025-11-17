package usace.cc.plugin.api.action_runner;

import usace.cc.plugin.api.Action;
import usace.cc.plugin.api.PluginManager;
import usace.cc.plugin.api.Logger;

public abstract class ActionRunnerBase {
    
    private String name;
    private PluginManager pm;
    private Action action;
    public Logger log;
    
    public PluginManager getPm() {
        return pm;
    }
    public void setPm(PluginManager pm) {
        this.pm = pm;
    }
    public Action getAction() {
        return action;
    }
    public void setAction(Action action) {
        this.action = action;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    
}
