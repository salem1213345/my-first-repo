# Architecture Diagram (Increment 3)

[FileLineSource] ---> (ParserStage) ---> (PredicateFilterStage) ---> (ProjectionStage) ---> [FileSink / ConsoleSink]

Composability achieved via PipelineBuilder and modular Stages.
