from abc import ABC, abstractmethod
import sys
import re
import os
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
# 2. Concrete Implementations (Increment 3)
# ---------------------------------------------------------

class FileLineSource(Source):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def produce(self, emitter: Emitter):
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                emitter.emit(line.rstrip('\n'))

class ParserStage(Stage):
    """تحويل سطر اللوج النصي إلى قاموس بيانات"""
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
    """تصفية السجلات بناءً على شرط معين"""
    def __init__(self, predicate):
        self.predicate = predicate

    def process(self, item, emitter: Emitter):
        if self.predicate(item):
            emitter.emit(item)

class ProjectionStage(Stage):
    """اختيار حقول محددة فقط من السجل"""
    def __init__(self, fields):
        self.fields = fields

    def process(self, item: dict, emitter: Emitter):
        projected = {k: item[k] for k in self.fields if k in item}
        emitter.emit(projected)

class FileSink(Sink):
    """مخرج يكتب النتائج داخل ملف"""
    def __init__(self, file_path: str):
        self.file_path = file_path
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
# 3. Pipeline & PipelineBuilder (Fluent Interface)
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

class PipelineBuilder:
    def __init__(self, source: Source):
        self.source = source
        self.stages = []

    @staticmethod
    def from_source(source: Source):
        return PipelineBuilder(source)

    def via(self, stage: Stage):
        self.stages.append(stage)
        return self

    def to(self, sink: Sink) -> Pipeline:
        return Pipeline(self.source, self.stages, sink)


# ---------------------------------------------------------
# 4. Main Entry Point (Command Line Options)
# ---------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="LogFlow Pipeline Increment 3")
    parser.add_argument("input_file", help="Path to input log file")
    parser.add_argument("--out", help="Path to output file (if not provided, outputs to console)")
    parser.add_argument("--min-status", type=int, help="Filter logs with status >= min-status")
    parser.add_argument("--path-prefix", help="Filter logs with path starting with prefix")
    parser.add_argument("--fields", help="Comma separated fields to project (e.g. timestamp,clientip,status)")

    args = parser.parse_args()

    builder = PipelineBuilder.from_source(FileLineSource(args.input_file)).via(ParserStage())

    if args.min_status is not None:
        builder.via(PredicateFilterStage(lambda r: r['status'] >= args.min_status))

    if args.path_prefix:
        builder.via(PredicateFilterStage(lambda r: r['path'].startswith(args.path_prefix)))

    if args.fields:
        field_list = [f.strip() for f in args.fields.split(',')]
        builder.via(ProjectionStage(field_list))

    sink = FileSink(args.out) if args.out else ConsoleSink()
    pipeline = builder.to(sink)

    pipeline.run()

if __name__ == "__main__":
    main()
