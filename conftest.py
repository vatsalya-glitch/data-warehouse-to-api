# Empty on purpose: its presence at the repo root tells pytest this is the
# rootdir, which puts the repo root on sys.path. Without it, pytest would
# resolve `tests/pipeline/` as the top-level package "pipeline" (since
# tests/ has no __init__.py of its own to disambiguate) and shadow the
# real pipeline/ package at the repo root, breaking every `from pipeline
# import ...` in tests/pipeline/test_pipeline.py. tests/__init__.py fixes
# the same issue from the other direction, by making "tests" itself a
# proper package so "tests.pipeline" and "pipeline" can't collide.
