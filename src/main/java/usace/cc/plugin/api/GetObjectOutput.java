package usace.cc.plugin.api;

import java.io.InputStream;

public class GetObjectOutput {

    private InputStream content;
    private String contentType;

    public GetObjectOutput(InputStream content, String contentType){
        this.content=content;
        this.contentType=contentType;
    }

    public InputStream getContent() {
        return content;
    }
    public void setContent(InputStream content) {
        this.content = content;
    }
    public String getContentType() {
        return contentType;
    }
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    
    
}
