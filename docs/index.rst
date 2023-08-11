 .. _reddit_decider:

``reddit_decider``
==================

.. automodule:: reddit_decider


Prerequisite packages
---------------------
.. code-block:: python

    baseplate>=2.0.0

    reddit-edgecontext>=1.0.0

    reddit-v2-events>=2.8.2

Prerequisite infrastructure
---------------------------
**Zookeeper live-data sidecar**:

Define a **live-data sidecar** in the manifest file to fetch the experiment configuration, see example setup `here
<https://reddit.atlassian.net/wiki/spaces/EX/pages/343212125/Running+Experiments+at+Reddit#Live-Data-Sidecar>`__
(make sure that your service is authorized to fetch the appropriate secret from Vault).

**Event publisher sidecar**:

Set up your service to be able to publish v2 exposure events via an **event publisher sidecar**, see example setup `here <https://reddit.atlassian.net/wiki/spaces/EX/pages/343212125/Running+Experiments+at+Reddit#Event-Publisher-Sidecar>`__.

Prerequisite configuration
---------------------------
Setup :code:`reddit-experiments` in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # optional: a path to the file where experiment configuration is written
   # default: /var/local/experiments.json
   # note: production systems load the experiments.json file under nested `live-data/` dir
   experiments.path = /var/local/live-data/experiments.json

   # optional: how long to wait for the experiments file to exist before failing
   # default:
   #    >= v1.7.0 wait 30 seconds
   #    <  v1.7.0 do not wait, fail immediately if not available
   experiments.timeout = 60 seconds

   # optional: the base amount of time for exponential backoff while waiting
   # for the file to be available.
   # default: no backoff time between tries
   experiments.backoff = 1 second

   ...


Integrate :code:`reddit-experiments` into Baseplate service
-----------------------------------------------------------

Upgrade or integrate reddit-experiments package:

.. code-block:: python

    # import latest reddit-experiments package in service requirements.txt
    reddit-experiments>=1.7.0

Initialize :code:`decider` instance on Baseplate context
--------------------------------------------------------

In your service's initialization process, add a :code:`decider` instance to baseplate's context:

.. code-block:: python

    # application code
    from event_utils.v2_event_utils import ExperimentLogger
    from reddit_decider import decider_client_from_config
    from reddit_decider import DeciderClient

    # optional
    from some_file import my_field_extractor


    def make_wsgi_app(app_config):
        baseplate = Baseplate(app_config)
        decider_factory = decider_client_from_config(app_config=app_config,
                                                     event_logger=ExperimentLogger(),
                                                     prefix="experiments.",
                                                     request_field_extractor=my_field_extractor)  # this is optional, can be `None` if edge_context contains all the fields you need
        baseplate.add_to_context("decider", decider_factory)

        # Or use `DeciderClient` with `configure_context()`,
        # which internally calls `decider_client_from_config()`
        baseplate.configure_context({
            "decider": DeciderClient(
                prefix="experiments.",
                event_logger=ExperimentLogger()),
                request_field_extractor=my_field_extractor  # optional
        })

Make sure :code:`EdgeContext` is accessible on :code:`request` object like so:

.. code-block:: python

    request.edge_context

If you **don't have access** to :code:`edge_context` in your service/request, you can access the SDK’s internal decider instance for a lower level API,
allowing you to pass in targeting context fields as a :code:`dict` param,
e.g. "user_is_employee", "country_code", or other targeting fields (instead of them being auto-derived from :code:`edge_context`).

See full API in `readme <https://github.snooguts.net/reddit/decider/tree/master/decider-py#class-decider>`_ (reddit internal).

The internal decider instance can be accessed from the SDK's top-level decider instance via:

.. code-block:: python

    internal_decider = request.decider.internal_decider()  # requires `reddit-experiments >= 1.4.1`
    internal_decider.choose("experiment_name", {
            "user_id": "t2_abc",
            "user_is_employee": True,
            "other_info": { "arbitrary_field": "some_val" }
        }
    )


[Optional] Define request field extractor function (`example <https://github.snooguts.net/reddit/reddit-service-graphql/blob/master/graphql-py/graphql_api/models/experiment.py#L67-L92>`_)

.. code-block:: python

    # Baseplate calls `make_object_for_context()` and creates a `DeciderContext`
    # which fetches the following fields from EdgeContext automatically:
    #   - user_id
    #   - device_id
    #   - logged_in
    #   - cookie_created_timestamp
    #   - oauth_client_id
    #   - country_code
    #   - locale
    #   - origin_service
    #   - is_employee
    #   - loid_created_ms (>=1.3.11)

    # Customized fields can be defined below to be extracted from a baseplate request
    # and will override above edge_context fields.
    # These fields may be used for targeting.

    def my_field_extractor(request):
        # an example of customized baseplate request field extractor:
        return {"foo": request.headers.get("Foo"), "bar": "something"}


Basic Usage
-----------
Use the attached :py:class:`~reddit_decider.Decider` instance in request to call
:code:`decider.get_variant()` (automatically sends an expose event)::

    def my_method(request):
        if request.decider.get_variant("foo") == "bar":
            ...

or optionally, if manual exposure is necessary, use::

    def my_method(request):
        variant = request.decider.get_variant_without_expose(experiment_name='experiment_name')
        ...
        request.decider.expose(experiment_name='experiment_name', variant_name=variant)

This is an example of using a dynamic configuration::

    def my_method(request):
        if request.decider.get_bool("foo") == True:
            ...


Decider API
-----------

.. autoclass:: Decider
   :members:

Configuration Class
-------------------

.. autoclass:: DeciderClient

Configuration Function
----------------------

.. autofunction:: decider_client_from_config


Configuration Context Factory
-----------------------------

.. autoclass:: DeciderContextFactory

Legacy API docs:
----------------

.. toctree::
  :maxdepth: 1

  legacy/index
