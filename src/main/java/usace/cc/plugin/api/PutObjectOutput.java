package usace.cc.plugin.api;

public class PutObjectOutput {

    private String filecode;
    private String hash;

    public PutObjectOutput(String code, String hash){
        this.filecode=code;
        this.hash=hash;
    }

    public String getFilecode() {
        return filecode;
    }

    public void setFilecode(String filecode) {
        this.filecode = filecode;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    
    
}
