# Dask-checkpoint

Dask-checkpoint is a Python package
that adds a customizable caching capabilities to [dask](https://dask.org).
It builds on top of `dask.delayed`,
adding load and save instructions
to the dask graph.

```python
from dask_checkpoint import Storage, task

storage = Storage.from_fsspec("my_directory")


@task(save=True)
def add_one(x):
    return x + 1


x0 = add_one(1).compute()  # computed
with storage():
    x1 = add_one(1).compute()  # computed and saved to storage
    x2 = add_one(1).compute()  # loaded from storage
x3 = add_one(1).compute()  # recomputed, not loaded from storage

assert x0 == x1 == x2 == x3
```

## Installation

Currently,
you have to install dask-checkpoint from GitHub.

```
pip install git+https://github.com/maurosilber/dask-checkpoint
```

## Getting started

Check out the [tutorial](examples/tutorial.ipynb) to see Dask-checkpoint in action.

## Development

To set up a development environment in a new conda environment,
run the following commands:

```
git clone https://github.com/maurosilber/dask-checkpoint
cd dask-checkpoint
conda env create -f environment-dev.yml
pre-commit install
```

Run tests locally with `tox`:

```
tox
```

or, if you have `mamba` installed:

```
CONDA_EXE=mamba tox
```
