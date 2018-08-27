package test;

import com.github.davidmoten.rx2.flowable.Transformers;
import io.reactivex.FlowableTransformer;

import java.util.TreeMap;

public class RxUtils {

    static FlowableTransformer<Indexed<byte[]>, Indexed<byte[]>> createReorderingTransformer() {

        return Transformers.stateMachine()
                .initialStateFactory(() -> new State(new TreeMap<>(), 0))
                .<Indexed<byte[]>, Indexed<byte[]>>transition((state, element, subscriber) -> {
                    System.out.println("State transition for element " + element + " on thread " + Thread.currentThread().getName());
                    int current = state.current;
                    TreeMap<Integer, Indexed<byte[]>> elements = state.getElements();

                    if (element.index == state.getCurrent()) {
                        subscriber.onNext(element);
                        current += 1;
                        while (elements.containsKey(current)) {
                            subscriber.onNext(elements.remove(current));
                            current += 1;
                        }
                        return new State(elements, current);
                    } else {
                        elements.put(element.index, element);
                        //maximumQueued.set(Math.max(maximumQueued.get(), elements.size()));
                        return new State(elements, current);
                    }
                }).build();
    }

    static class State {
        private TreeMap<Integer, Indexed<byte[]>> elements;
        private int current;

        public State(TreeMap<Integer, Indexed<byte[]>> elements, int current) {
            this.elements = new TreeMap<>(elements);
            this.current = current;
        }

        public TreeMap<Integer, Indexed<byte[]>> getElements() {
            return new TreeMap<>(elements);
        }

        public int getCurrent() {
            return current;
        }
    }


}
