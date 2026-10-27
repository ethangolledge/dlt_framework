"""A terminal-styled dashboard over the framework's monitoring frames.

Layers, each importing only those below it: ``app`` (frontends) → ``pages`` → ``model`` (frames
to facts, pure Polars) and ``render`` (ASCII primitives to styled text) → ``theme``. It reads
the frames through the framework's sinks and nothing else, so it works for any pipeline and
source type, wherever the frames can be read.
"""
