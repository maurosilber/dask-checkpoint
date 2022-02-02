# Pipeline

Pipeline is a Python package to build complex analysis pipelines with [dask](https://dask.org).

Inspired on spotify's [Luigi](https://github.com/spotify/luigi), Pipeline builds on top of `dask.delayed`, adding save and load instructions to the dask graph.

In contrast to Luigi, Pipeline allows to experiment with the analysis pipeline more interactively, thanks to dask. For instance, within a Jupyter notebook.

At the same time, it can also accelerate experimenting with the analysis by storing expensive intermediate computations to disk, in contrast to plain dask.

## Installation

Currently, you have to install pipeline from GitHub.

```
pip install git+https://github.com/maurosilber/pipeline
```

## Getting started

Check out the [tutorial](examples/tutorial.ipynb) to see Pipeline in action.

## Development

To set up a development environment in a new conda environment, run the following commands:

```
git clone https://github.com/maurosilber/pipeline
cd pipeline
conda env create -f environment.yml
pip install -e .[dev]
pre-commit install
```
