"""A terminal-styled dashboard over the framework's monitoring frames.

Layers, each importing only those below it: ``app`` (the command line) → ``web`` (the data
snapshot and the browser page) → ``model`` (frames to facts, pure Polars) and ``render`` (styled
terminal text) → ``theme``. It reads the frames through the framework's sinks and nothing else,
so it works for any pipeline and source type, wherever the frames can be read.
"""
