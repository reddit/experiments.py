``reddit_experiments (legacy)``
===============================

.. automodule:: reddit_experiments

Please consider upgrading your service to use the latest :ref:`reddit_decider` SDK
to gain access to new features, such as **Mutually Exclusive Groups**, **Holdout Groups**, and **Dynamic Configurations**.


Initialize :py:class:`~reddit_experiments.Experiments` instance on Baseplate context
------------------------------------------------------------------------------------

Add the :code:`Experiments` client to your application via::

   baseplate.configure_context(
      {
         ...
         "experiments": ExperimentsClient(event_logger),
         ...
      }
   )

or alternatively using::

    experiments_factory = experiments_client_from_config(
                              app_config=app_config,
                              event_logger=ExperimentLogger
    )
    baseplate.add_to_context("experiments", experiments_factory)


Configure :py:class:`~reddit_experiments.Experiments` client
------------------------------------------------------------

Configure the client in your application's configuration file:

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


Usage
-----
Use the attached :py:class:`~reddit_experiments.Experiments` object in
request to get a variant::

   def my_method(request):
       if request.experiments.variant("foo") == "bar":
           pass

Experiments API
---------------

.. autoclass:: Experiments
   :members:

Configuration Class
-------------------

.. autoclass:: ExperimentsClient

Configuration Function
----------------------

.. autofunction:: experiments_client_from_config

Configuration Context Factory
-----------------------------

.. autoclass:: ExperimentsContextFactory

Link to Latest SDK
------------------
See :ref:`reddit_decider`
