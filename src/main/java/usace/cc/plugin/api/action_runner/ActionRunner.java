package usace.cc.plugin.api.action_runner;

public interface ActionRunner {

    public static class ActionRunnerException extends Exception{
        public ActionRunnerException(Exception ex){
            super(ex);
        }

        public ActionRunnerException(String msg){
            super(msg);
        }
    }

    public void Run() throws ActionRunnerException;
    public String GetName();
}