package clio.io.impl;

import clio.io.AbstractCloneablePipeline;
import clio.io.control_plane.CloneConfig;
import clio.io.interfaces.DispatchPreProcess;
import clio.io.interfaces.SlotManager;

public class FunctionalPipeline extends AbstractCloneablePipeline {

    public FunctionalPipeline(String name, CloneConfig cloneConfig,
            DispatchPreProcess preProcess,
            SlotManager slotManager,
            FunctionalExecutor executor) {
        super(name, cloneConfig, preProcess, slotManager, executor);
    }

    @Override
    public FunctionalPipeline hookOnClone(CloneConfig cloneConfig) {
        return new FunctionalPipeline(name, cloneConfig, preProcess, slotManager,
                (FunctionalExecutor) executor);
    }
}
