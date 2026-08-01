"""Operator and deploy scripts.

A package rather than loose files so ``python -m scripts.seed_fetch`` and
``from scripts.seed_fetch import ...`` resolve the same way, rather than one of
them depending on namespace-package discovery and the working directory.

``sync_wheelhouse.py`` deliberately does not import the project — it runs before
``uv sync`` in an isolated environment — and is invoked by path, so it is
unaffected by living in a package.
"""
