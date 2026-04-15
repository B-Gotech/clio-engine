package clio.io.interfaces;

import clio.io.control_plane.CloneConfig;
import clio.io.frames.AbstractFrame;
import clio.io.utils.PinnedThreadExecutor;

public interface PipelineExecutor extends CloneableObject {
    void reportErrorsTo(CloneableObject clone);

    void execute(AbstractFrame frame);

    PipelineExecutor clone(CloneConfig config);

    PipelineExecutor clone(CloneConfig config, PinnedThreadExecutor executorService);
}
