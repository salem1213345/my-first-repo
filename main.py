from abc import ABC, abstractmethod
import sys
import re
import os
import json
import argparse

# ---------------------------------------------------------
# 1. Core Interfaces & LifeCycle Hooks
# ---------------------------------------------------------

class Emitter(ABC):
    @abstractmethod
    def emit(self, item):
        pass

class Source(ABC):
    @abstractmethod
    def produce(self, emitter: Emitter):
        pass

class Sink(ABC):
    def open(self):
        pass

    @abstractmethod
    def consume(self, item):
        pass

    def close(self):
        pass

class Stage(ABC):
    def open(self):
        pass

    @abstractmethod
    def process(self, item, emitter: Emitter):
        pass

    def close(self):
        pass


# ---------------------------------------------------------
# 2. Concrete Implementations
# ---------------------------------------------------------

class FileLineSource(Source):
    def __init__(self, path: str):
        self.file_path = path

    def produce(self, emitter: Emitter):
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                emitter.emit(line.rstrip('\n'))

class ParserStage(Stage):
    LOG_PATTERN = re.compile(
        r'^(?P<clientip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<verb>\S+) (?P<path>\S+) \S+" (?P<status>\d+) (?P<bytes>\d+)'
    )

    def process(self, item: str, emitter: Emitter):
        match = self.LOG_PATTERN.match(item)
        if match:
            data = match.groupdict()
            data['status'] = int(data['status'])
            data['bytes'] = int(data['bytes'])
            emitter.emit(data)

class PredicateFilterStage(Stage):
    def __init__(self, field: str, op: str, value):
        self.field = field
        self.op = op
        self.value = value

    def process(self, item: dict, emitter: Emitter):
        if self.field not in item:
            return
        val = item[self.field]
        match = False
        if self.op == ">=" and val >= self.value:
            match = True
        elif self.op == "==" and val == self.value:
            match = True
        elif self.op == "startswith" and str(val).startswith(str(self.value)):
            match = True

        if match:
            emitter.emit(item)

class ProjectionStage(Stage):
    def __init__(self, fields: list):
        self.fields = fields

    def process(self, item: dict, emitter: Emitter):
        projected = {k: item[k] for k in self.fields if k in item}
        emitter.emit(projected)

class FileSink(Sink):
    def __init__(self, path: str):
        self.file_path = path
        self.file = None

    def open(self):
        dirname = os.path.dirname(self.file_path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)
        self.file = open(self.file_path, 'w', encoding='utf-8')

    def consume(self, item):
        if self.file:
            self.file.write(str(item) + '\n')

    def close(self):
        if self.file:
            self.file.close()

class ConsoleSink(Sink):
    def consume(self, item):
        print(item)


# ---------------------------------------------------------
# 3. Stage Registry & Pipeline Loader (Increment 4)
# ---------------------------------------------------------

class StageRegistry:
    _stages = {}
    _sources = {}
    _sinks = {}

    @classmethod
    def register_stage(cls, name, stage_cls):
        cls._stages[name] = stage_cls

    @classmethod
    def register_source(cls, name, source_cls):
        cls._sources[name] = source_cls

    @classmethod
    def register_sink(cls, name, sink_cls):
        cls._sinks[name] = sink_cls

    @classmethod
    def create_stage(cls, name, params):
        if name not in cls._stages:
            raise ValueError(f"Config Error: Unknown stage type '{name}'")
        try:
            return cls._stages[name](**params)
        except TypeError as e:
            raise ValueError(f"Config Error: Invalid parameters for stage '{name}': {e}")

    @classmethod
    def create_source(cls, name, params):
        if name not in cls._sources:
            raise ValueError(f"Config Error: Unknown source type '{name}'")
        try:
            return cls._sources[name](**params)
        except TypeError as e:
            raise ValueError(f"Config Error: Missing or invalid parameters for source '{name}': {e}")

    @classmethod
    def create_sink(cls, name, params):
        if name not in cls._sinks:
            raise ValueError(f"Config Error: Unknown sink type '{name}'")
        try:
            return cls._sinks[name](**params)
        except TypeError as e:
            raise ValueError(f"Config Error: Missing or invalid parameters for sink '{name}': {e}")


# Data-driven Registration
StageRegistry.register_source("file", FileLineSource)
StageRegistry.register_sink("file", FileSink)
StageRegistry.register_sink("console", ConsoleSink)
StageRegistry.register_stage("parse", ParserStage)
StageRegistry.register_stage("filter", PredicateFilterStage)
StageRegistry.register_stage("project", ProjectionStage)


class PipelineLoader:
    @staticmethod
    def load_from_file(config_path: str):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config Error: Config file not found at '{config_path}'")

        with open(config_path, 'r', encoding='utf-8-sig') as f:
            try:
                data = json.load(f)
            except Exception as e:
                raise ValueError(f"Config Error: Invalid JSON configuration format: {e}")

        if "pipeline" not in data:
            raise ValueError("Config Error: Missing root 'pipeline' key")

        p_config = data["pipeline"]
        
        if "source" not in p_config:
            raise ValueError("Config Error: Missing 'source' configuration")
        source_cfg = p_config["source"]
        source = StageRegistry.create_source(source_cfg.get("type"), source_cfg.get("params", {}))

        stages_cfg = p_config.get("stages", [])
        if not stages_cfg:
            raise ValueError("Config Error: Pipeline must contain at least one stage (empty pipeline)")

        stages = []
        for s_cfg in stages_cfg:
            stage_type = s_cfg.get("type")
            stage_params = s_cfg.get("params", {})
            stage = StageRegistry.create_stage(stage_type, stage_params)
            stages.append(stage)

        if "sink" not in p_config:
            raise ValueError("Config Error: Missing 'sink' configuration")
        sink_cfg = p_config["sink"]
        sink = StageRegistry.create_sink(sink_cfg.get("type"), sink_cfg.get("params", {}))

        return Pipeline(source, stages, sink)


# ---------------------------------------------------------
# 4. Pipeline Execution Structure
# ---------------------------------------------------------

class StageEmitter(Emitter):
    def __init__(self, next_stage: Stage, next_emitter: Emitter):
        self.next_stage = next_stage
        self.next_emitter = next_emitter

    def emit(self, item):
        self.next_stage.process(item, self.next_emitter)

class SinkEmitter(Emitter):
    def __init__(self, sink: Sink):
        self.sink = sink

    def emit(self, item):
        self.sink.consume(item)

class Pipeline:
    def __init__(self, source: Source, stages: list, sink: Sink):
        self.source = source
        self.stages = stages
        self.sink = sink

    def run(self):
        self.sink.open()
        for stage in self.stages:
            stage.open()

        current_emitter = SinkEmitter(self.sink)
        for stage in reversed(self.stages):
            current_emitter = StageEmitter(stage, current_emitter)

        self.source.produce(current_emitter)

        for stage in self.stages:
            stage.close()
        self.sink.close()


# ---------------------------------------------------------
# 5. Main Entry Point
# ---------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="LogFlow Pipeline Increment 4")
    parser.add_argument("--config", required=True, help="Path to JSON configuration file")

    args = parser.parse_args()

    try:
        pipeline = PipelineLoader.load_from_file(args.config)
        pipeline.run()
        print(f"Pipeline executed successfully using configuration: {args.config}")
    except ValueError as ve:
        print(f"[FAIL FAST CONFIG ERROR]: {ve}")
        sys.exit(1)
    except FileNotFoundError as fnf:
        print(f"[FAIL FAST FILE ERROR]: {fnf}")
        sys.exit(1)

if __name__ == "__main__":
    main()