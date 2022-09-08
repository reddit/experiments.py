# experiments.py

Experiments allow us to determine the impact of changes we make. This library
helps you run and track them in Baseplate.py services.

Documentation: https://reddit-experiments.readthedocs.io/

## Usage

Install the library:

```console
$ pip install reddit-experiments
```

Add the client to your application's Baseplate context:

```python
 from reddit_decider import decider_client_from_config

 decider = decider_client_from_config(
     app_config=app_config,
     event_logger=ExperimentLogger(),
     request_field_extractor=decider_field_extractor,
 )
 baseplate.add_to_context("decider", decider)
```

and use it in request:

```python
def my_method(request):
   if request.decider.get_variant("foo") == "bar":
       pass
```

See [the documentation] for more information (documentation builds can be found [here](https://readthedocs.org/projects/reddit-experiments/builds/))
.

[the documentation]: https://reddit-experiments.readthedocs.io/

## Development

A Dockerfile is provided to get a development environment running. To use it,
build the base Docker image:

```console
$ docker build -t experiments .
```

And then fire up the environment and use the provided Makefile targets to do
common tasks:

```console
$ docker run -it -v $PWD:/src -w /src experiments
$ make fmt
```

The following make targets are provided:

* `fmt`: Apply automatic formatting to the source code.
* `lint`: Run linters on the code.
* `test`: Run the test suite.
* `docs`: Build the docs. Output can be found in `build/html/`.

Note: some tests are skipped by default locally because they are quite slow.
Enable these by setting CI=true in the environment: `CI=true make test`.
