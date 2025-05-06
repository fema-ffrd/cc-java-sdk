package usace.cc.plugin.api.utils;

import java.util.ArrayList;
import java.util.List;

public class ResourceScope implements AutoCloseable {
    private final List<AutoCloseable> resources = new ArrayList<>();

    public <T extends AutoCloseable> T add(T resource) {
        resources.add(resource);
        return resource;
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        for (int i = resources.size() - 1; i >= 0; i--) {
            try {
                resources.get(i).close();
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null) throw first;
    }
}