``reddit_experiments``
===============================

.. automodule:: reddit_experiments

Example
-------

To add the experiments client to your application, add the appropriate client
declaration to your context configuration::

   baseplate.configure_context(
      {
         ...
         "experiments": ExperimentsClient(event_logger),
         ...
      }
   )

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

and then use the attached :py:class:`~reddit_experiments.Experiments` object in
request::

   def my_method(request):
       if request.experiments.variant("foo") == "bar":
           pass

Configuration
-------------

.. autoclass:: ExperimentsClient

.. autofunction:: experiments_client_from_config

Classes
-------

.. autoclass:: ExperimentsContextFactory

.. autoclass:: Experiments
   :members:
