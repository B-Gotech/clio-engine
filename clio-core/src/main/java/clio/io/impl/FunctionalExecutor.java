package clio.io.impl;

import clio.io.AbstractExecutor;
import clio.io.control_plane.CloneConfig;
import clio.io.frames.AbstractFrame;
import clio.io.frames.ConsumerFrame;
import clio.io.frames.FunctionFrame;
import clio.io.frames.RunnableFrame;
import clio.io.frames.SequencedFrame;
import clio.io.utils.PinnedThreadExecutor;

public class FunctionalExecutor extends AbstractExecutor {
    FunctionalExecutor(PinnedThreadExecutor executorService) {
        super(executorService);
    }

    @Override
    public void execute(AbstractFrame frame) {
        if (!frame.isAlive()) {
            frame.throwMeAsError();
        }

        if(frame instanceof SequencedFrame s) {
            s.apply();
        } else if (frame instanceof ConsumerFrame c) {
            c.consume();
        } else if (frame instanceof FunctionFrame f) {
            f.apply();
        } else if (frame instanceof RunnableFrame r) {
            r.run();
        } else {
            frame.throwMeAsError();
        }
    }

    @Override
    public FunctionalExecutor clone(CloneConfig cloneConfig) {
        return new FunctionalExecutor(executorService);
    }

    @Override
    public FunctionalExecutor clone(CloneConfig cloneConfig, PinnedThreadExecutor executor) {
        return new FunctionalExecutor(executor);
    }

}
