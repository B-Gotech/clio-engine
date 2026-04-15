package clio.io.resource_monitoring.providers;

import clio.io.resource_monitoring.SystemUtilization.SystemSnapshot;

public interface ResourceProvider {
    SystemSnapshot getSnapshot();
}
