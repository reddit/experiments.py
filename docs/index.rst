``reddit_experiments``
===============================

.. automodule:: reddit_experiments

Example
-------

To add the experiments client to your application, add the appropriate client
declaration to your context configuration::

   from reddit_decider import decider_client_from_config
    
    decider = decider_client_from_config(
        app_config=app_config,
        event_logger=ExperimentLogger(),
        request_field_extractor=decider_field_extractor,
    )
    baseplate.add_to_context("decider", decider)

configure it in your application's configuration file:

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

and then use the attached :py:class:`~reddit_decider.Decider` object in
request::

   def my_method(request):
       if request.decider.get_variant("foo") == "bar":
           pass

Configuration
-------------

.. autoclass:: DeciderClient

.. autofunction:: decider_client_from_config

Classes
-------

.. autoclass:: DeciderContextFactory

.. autoclass:: Decider
   :members:
