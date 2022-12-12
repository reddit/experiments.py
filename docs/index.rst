 .. _reddit_decider:

``reddit_decider``
===============================

.. automodule:: reddit_decider


Prerequisite packages
---------------------
.. code-block:: python

    baseplate>=2.0.0

    reddit-edgecontext>=1.0.0

    # upgrade or integrate latest reddit-v2-events package
    # or manually update thrift schemas
    # to allow event fields to be populated in exposures
    reddit-v2-events

Prerequisite infrastructure
---------------------------
**Zookeeper live-data sidecar**:

Set up your service to pull down & synchronize experiment configurations from Zookeeper via the Baseplate `live-data watcher sidecar
<https://baseplate.readthedocs.io/en/stable/api/baseplate/lib/live_data.html?highlight=sidecar#watcher-daemon>`_ (minimum v2.5.4).
You'll have to make sure that your service is authorized to fetch the appropriate secret from Vault.
See example setup `here
<https://reddit.atlassian.net/wiki/spaces/EX/pages/343212125/Running+Experiments+at+Reddit#Live-Data-Sidecar>`__.

**Event publisher sidecar**:

Set up your service to be able to publish v2 exposure events via an `events sidecar <https://baseplate.readthedocs.io/en/stable/api/baseplate/lib/events.html?highlight=sidecar#publishing-events>`_
. See example setup `here <https://reddit.atlassian.net/wiki/spaces/EX/pages/343212125/Running+Experiments+at+Reddit#Event-Publisher-Sidecar>`__.

Prerequisite configuration
---------------------------
Setup :code:`reddit-experiments` in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # optional: a path to the file where experiment configuration is written
   # (default: /var/local/experiments.json)
   experiments.path = /var/local/foo.json

   # optional: how long to wait for the experiments file to exist before failing
   # (default: do not wait. fail immediately if not available)
   experiments.timeout = 60 seconds

   # optional: the base amount of time for exponential backoff while waiting
   # for the file to be available.
   # (default: no backoff time between tries)
   experiments.backoff = 1 second

   ...


Integrate :code:`reddit-experiments` into Baseplate service
-----------------------------------------------------------

Upgrade or integrate reddit-experiments package:

.. code-block:: python

    # import latest reddit-experiments package in service requirements.txt
    reddit-experiments>=1.3.11

Initialize :code:`decider` instance on Baseplate context
--------------------------------------------------------

In your service's initialization process, add a :code:`decider` instance to baseplate's context:
(Note the use of the :code:`ExperimentLogger`, which is used to publish exposure V2 events,
an example can be seen `here <https://github.snooguts.net/reddit/reddit-service-graphql/blob/master/graphql-py/graphql_api/events/utils.py>`_)

.. code-block:: python

    # application code
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
                event_logger=ExperimentLogger,
                request_field_extractor=my_field_extractor  # optional
        })

Make sure :code:`EdgeContext` is accessible on :code:`request` object like so:

.. code-block:: python

    request.edge_context


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
Use the attached :py:class:`~reddit_decider.Decider` object in request to call
:code:`decider.get_variant()` (automatically sends an expose event)::

    def my_method(request):
        if request.decider.get_variant("foo") == "bar":
            ...

or optionally, if manual exposure is necessary, use::

    def my_method(request):
        variant = request.decider.get_variant_without_expose(experiment_name='experiment_name')
        ...
        request.decider.expose(experiment_name='experiment_name', variant_name=variant)

and this is an example of using a dynamic configuration::

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
