# Perception Neuron to LSL relay

## Install

```
$ git clone https://github.com/jamieforth/pylsl-pn.git
$ cd pylsl-pn
$ uv sync
```

## Updating

```
$ git pull
$ uv sync
```

## Running tests

```
$ uv run pytest
```

## Running relay

To print help and additional options:

```
$ uv run lsl-pn --help
```

Run relay with default options (ctrl-c to quit):

```
$ uv run lsl-pn
```
