package com.linkedin.thirdeye.rootcause;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Container class for configuring and executing a root cause search with multiple pipelines.
 * The framework is instantiated with multiple (named) pipelines and a result aggregator. The run()
 * method then executes the configured pipelines for arbitrary inputs without
 * maintaining any additional state within the RCAFramework.
 *
 * RCAFramework supports parallel DAG execution and requires pipelines to form a valid path
 * from {@code INPUT} to {@code OUTPUT}. The execution order of pipelines is guaranteed to be
 * compatible with serial execution in one single thread.
 */

/*
 *                   /-> pipeline.run() --> pipeline.run() \
 *                  /                                       \
 * INPUT --> run() ---> pipeline.run() ---> pipeline.run() --> OUTPUT
 *                  \                    /
 *                   \-> pipeline.run() /
 */
public class RCAFramework {
  private static final Logger LOG = LoggerFactory.getLogger(RCAFramework.class);

  public static final String INPUT = "INPUT";
  public static final String OUTPUT = "OUTPUT";
  public static final long TIMEOUT = 600000;

  final Map<String, Pipeline> pipelines;
  final ExecutorService executor;

  public RCAFramework(Collection<Pipeline> pipelines, ExecutorService executor) {
    this.executor = executor;

    if(!isValidDAG(pipelines))
      throw new IllegalArgumentException(String.format("Invalid DAG. '%s' not reachable from '%s'", OUTPUT, INPUT));

    this.pipelines = new HashMap<>();
    for(Pipeline p : pipelines) {
      if(INPUT.equals(p.getName()))
        throw new IllegalArgumentException(String.format("Must not contain a pipeline with name '%s'", INPUT));
      if(this.pipelines.containsKey(p.getName()))
        throw new IllegalArgumentException(String.format("Already contains pipeline with name '%s'", p.getName()));
      this.pipelines.put(p.getName(), p);
    }

    if(!this.pipelines.containsKey(OUTPUT))
      throw new IllegalArgumentException(String.format("Must contain a pipeline named '%s'", OUTPUT));
  }

  /**
   * Performs rootcause search for a user-specified set of input entities.
   * Fans out entities to individual pipelines, collects results, and aggregates them.
   *
   * @param input user-specified search entities
   * @return aggregated results
   */
  public RCAFrameworkExecutionResult run(Set<Entity> input) throws Exception {
    Map<String, Pipeline> pipelines = new HashMap<>(this.pipelines);
    pipelines.put(INPUT, new StaticPipeline(INPUT, Collections.<String>emptySet(), input));

    LOG.info("Constructing flow for input '{}'", input);
    Map<String, Future<PipelineResult>> flow = constructDAG(pipelines);

    Map<String, PipelineResult> results = new HashMap<>();
    for(Map.Entry<String, Future<PipelineResult>> e : flow.entrySet()) {
      PipelineResult r = e.getValue().get(TIMEOUT, TimeUnit.MILLISECONDS);
      if(LOG.isDebugEnabled())
        logResultDetails(r);
      results.put(e.getKey(), r);
    }

    return new RCAFrameworkExecutionResult(results.get(OUTPUT).getEntities(), results);
  }

  static void logResultDetails(PipelineResult result) {
    List<Entity> entities = new ArrayList<>(result.getEntities());
    Collections.sort(entities, new Comparator<Entity>() {
      @Override
      public int compare(Entity o1, Entity o2) {
        return -Double.compare(o1.getScore(), o2.getScore());
      }
    });

    for(Entity e : entities) {
      LOG.debug("{} [{}] {}", Math.round(e.getScore() * 1000) / 1000.0, e.getClass().getSimpleName(), e.getUrn());
    }
  }

  static boolean isValidDAG(Collection<Pipeline> pipelines) {
    Set<String> visited = new HashSet<>();
    visited.add(INPUT);

    int prevSize = 0;
    while(prevSize < visited.size()) {
      prevSize = visited.size();
      for (Pipeline p : pipelines) {
        if (visited.containsAll(p.getInputs()))
          visited.add(p.getName());
      }
    }

    return visited.contains(OUTPUT);
  }

  Map<String, Future<PipelineResult>> constructDAG(Map<String, Pipeline> pipelines) {
    // TODO purge pipelines not on critical path
    Map<String, Future<PipelineResult>> tasks = new HashMap<>();
    Pipeline input = pipelines.get(INPUT);
    PipelineCallable inputCallable = new PipelineCallable(Collections.<String, Future<PipelineResult>>emptyMap(), input);
    tasks.put(INPUT, this.executor.submit(inputCallable));

    int prevSize = 0;
    while(prevSize < tasks.size()) {
      prevSize = tasks.size();
      for(Pipeline p : pipelines.values()) {
        if(!tasks.containsKey(p.getName()) && tasks.keySet().containsAll(p.getInputs())) {
          Map<String, Future<PipelineResult>> dependencies = new HashMap<>();
          for(String inputName : p.getInputs()) {
            dependencies.put(inputName, tasks.get(inputName));
          }

          PipelineCallable c = new PipelineCallable(dependencies, p);
          tasks.put(p.getName(), this.executor.submit(c));
        }
      }
    }

    return tasks;
  }
}