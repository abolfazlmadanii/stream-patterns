package io.github.abolfazlmadanii.streampatterns.testkit;

import java.util.List;

/**
 * Reference hook for comparing pipeline output to trusted expectations.
 *
 * @param <I> input event type fed to the pipeline under test
 * @param <O> output type produced by the pipeline
 */
public interface Oracle<I, O> {

  /** Returns the expected outputs for the given inputs in order. */
  List<O> expected(List<I> inputs);
}
