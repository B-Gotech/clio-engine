package clio.io.interfaces;

import clio.io.control_plane.CloneConfig;
import clio.io.utils.PinnedThreadExecutor;

public interface SlotManager extends CloneableObject {

    double getPressure();

    PinnedThreadExecutor getPinnedExecutor();

    SlotManager clone(CloneConfig cloneConfig);
}
