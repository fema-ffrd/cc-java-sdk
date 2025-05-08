package usace.cc.plugin.api.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Adds a resource to this {@code ResourceScope} to be automatically closed when the scope is closed.
 * <p>
 * Resources are closed in the reverse order of addition, ensuring that dependencies between them
 * are respected during cleanup.
 *
 * @param <T>      the type of the resource, which must implement {@link AutoCloseable}
 * @param resource the resource to be added to the scope
 * @return the same {@code resource}, for convenient in-line use
 * @throws NullPointerException if the {@code resource} is {@code null}
 */
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