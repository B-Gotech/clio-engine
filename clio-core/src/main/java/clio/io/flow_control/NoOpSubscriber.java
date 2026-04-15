package clio.io.flow_control;

import clio.io.frames.AbstractFrame;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public final class NoOpSubscriber implements Subscriber<AbstractFrame> {

    @Override
    public void onSubscribe(Subscription subscription) {

    }

    @Override
    public void onNext(AbstractFrame abstractFrame) {

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }
}
