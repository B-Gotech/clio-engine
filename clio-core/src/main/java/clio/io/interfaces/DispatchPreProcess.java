package clio.io.interfaces;

import clio.io.control_plane.CloneConfig;
import java.util.concurrent.Callable;

public interface DispatchPreProcess extends CloneableObject {

    void setDownstreamPressureMonitor(Callable<Double> pressure);

    DispatchPreProcess clone(CloneConfig cloneConfig);
}
