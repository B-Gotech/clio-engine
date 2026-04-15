package clio.io.test_utils;

import clio.io.AbstractCloneablePipeline;
import clio.io.AbstractExecutor;
import clio.io.control_plane.CloneConfig;
import clio.io.frames.AbstractFrame;
import clio.io.interfaces.DispatchPreProcess;
import clio.io.interfaces.PipelineExecutor;
import clio.io.interfaces.SlotManager;
import clio.io.utils.PinnedThreadExecutor;
import org.openjdk.jmh.infra.Blackhole;

public class TestPipeline extends AbstractCloneablePipeline {

    private final String name;

    public TestPipeline(String name, CloneConfig config, DispatchPreProcess scheduler,
            SlotManager slotManager, PipelineExecutor executor) {
        super(name, config, scheduler, slotManager, executor);
        this.name = name;
    }

    @Override
    public TestPipeline hookOnClone(CloneConfig cloneConfig) {
        return new TestPipeline(name, cloneConfig, this.preProcess, this.slotManager,
                this.executor);
    }

    public static class TestExecutor extends AbstractExecutor {

        private final PinnedThreadExecutor executor;
        private final Blackhole bh;

        public TestExecutor(PinnedThreadExecutor executor, Blackhole bh) {
            super(executor);
            this.executor = executor;
            this.bh = bh;
        }

        @Override
        public void execute(AbstractFrame frame) {
            if(bh != null) {
                bh.consume(frame);
            }
        }

        @Override
        public AbstractExecutor clone(CloneConfig cloneConfig) {
            return new TestExecutor(this.executor, this.bh);
        }

        @Override
        public AbstractExecutor clone(CloneConfig cloneConfig,
                PinnedThreadExecutor executor) {
            return new TestExecutor(executor, this.bh);
        }
    }
}
