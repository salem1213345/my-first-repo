# Architecture Diagram (Increment 4)

[Declarative Config File (JSON)] ---> [PipelineLoader]
                                              |
                                              v
                                      [StageRegistry]
                                              |
                                              v
[FileLineSource] ---> (ParserStage) ---> (PredicateFilterStage) ---> (ProjectionStage) ---> [FileSink / ConsoleSink]

Extensibility through Data-driven Stage Registry and Dynamic Pipeline Loader with Fail-Fast Configuration Validation.
