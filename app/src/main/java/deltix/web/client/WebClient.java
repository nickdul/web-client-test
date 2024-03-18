package deltix.web.client;

import java.io.Closeable;

public interface WebClient extends Closeable {
    void executeRequest();
}
