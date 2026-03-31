package com.ccri.clio.common.io.dispatch.interfaces;

import com.ccri.clio.common.io.dispatch.control_plane.CloneConfig;
import com.ccri.clio.common.io.dispatch.utils.PinnedThreadExecutor;

public interface SlotManager extends CloneableObject, AutoCloseable {

    double getPressure();

    PinnedThreadExecutor getPinnedExecutor();

    SlotManager clone(CloneConfig cloneConfig);
}
