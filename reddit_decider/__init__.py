import logging

from typing import Any, Callable, Dict, Optional

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.frameworks.pyramid import BaseplateRequest
from baseplate.lib import config
from baseplate.lib.events import DebugLogger
from baseplate.lib.events import EventLogger
from baseplate.lib.file_watcher import FileWatcher
from baseplate.lib.file_watcher import WatchedFileNotAvailableError

import rust_decider


logger = logging.getLogger(__name__)

EMPLOYEE_ROLES = ("employee", "contractor")
EVENT_TYPE = "expose"


class DeciderContext:
    """DeciderContext() is used to contain all fields necessary for
    bucketing, targeting, and overrides.
    DeciderContext() is populated in `make_object_for_context()`.
    """

    def __init__(
        self,
        user_id: str,
        country_code: Optional[str] = None,
        locale: Optional[str] = None,
        user_is_employee: Optional[bool] = None,
        logged_in: Optional[bool] = None,
        device_id: Optional[str] = None,
        request_url: Optional[str] = None,
        authentication_token: Optional[str] = None,
        app_name: Optional[str] = None,
        build_number: Optional[str] = None,
        origin_service: Optional[str] = None,
        cookie_created_timestamp: Optional[float] = None,
    ):
        self._user_id = user_id
        self._country_code = country_code
        self._locale = locale
        self._user_is_employee = user_is_employee
        self._logged_in = logged_in
        self._device_id = device_id
        self._request_url = request_url
        self._authentication_token = authentication_token
        self._app_name = app_name
        self._build_number = build_number
        self._origin_service = origin_service
        self._cookie_created_timestamp = cookie_created_timestamp


    def to_dict(self) -> Dict:
        return {
            "user_id": self._user_id,
            "country_code": self._country_code,
            "locale": self._locale,
            "user_is_employee": self._user_is_employee,
            "logged_in": self._logged_in,
            "device_id": self._device_id,
            "request_url": self._request_url,
            "authentication_token": self._authentication_token,
            "app_name": self._app_name,
            "build_number": self._build_number,
            "origin_service": self._origin_service,
            "cookie_created_timestamp": self._cookie_created_timestamp,
        }


def init_decider_parser(file):
    return rust_decider.init("darkmode overrides targeting fractional_availability value", file.name)

def validate_decider(decider: Optional[Any]) -> None:
    if decider is None:
        logger.error(f"Rust decider did not initialize.")

    if decider:
        decider_err = decider.err()
        logger.error(f"Rust decider has error: {decider_err}")


class Decider:
    """Access to experiments with automatic refresh when changed.

    This experiments client allows access to the experiments cached on disk by
    the experiment configuration fetcher daemon.  It will automatically reload
    the cache when changed.  This client also handles logging bucketing events
    to the event pipeline when it is determined that the request is part of an
    active variant.
    """

    def __init__(
        self,
        decider_context: DeciderContext,
        config_watcher: FileWatcher,
        server_span: Span,
        context_name: str,
        event_logger: Optional[EventLogger] = None,
    ):
        self._decider_context = decider_context
        self._config_watcher = config_watcher
        self._span = server_span
        self._context_name = context_name
        if event_logger:
            self._event_logger = event_logger
        else:
            self._event_logger = DebugLogger()

    def _get_decider(self):
        decider = None
        try:
            decider = self._config_watcher.get_data()
            validate_decider(decider)
            return decider
        except WatchedFileNotAvailableError as exc:
            logger.warning("Experiment config unavailable: %s", str(exc))
        except TypeError as exc:
            logger.warning("Could not load experiment config: %s", str(exc))
        return None

    def get_variant(
        self,
        experiment_name: Optional[str] = None,
        **exposure_kwargs: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        """Return a bucketing variant, if any, with auto-exposure.

        Since calling get_variant() will fire an exposure event, it
        is best to call this when you are making the decision that
        will expose the experiment to the user.
        If you absolutely must check the status of an experiment
        before you are sure that the experiment will be exposed to the user,
        you can use `get_variant_without_expose()` to disable exposure events
        and call `expose()` manually later.

        :param experiment_name: Name of the experiment you want to run.

        :param exposure_kwargs:  Additional arguments that will be passed
            to events_logger under "inputs" key.

        :return: Variant name if a variant is assigned, None otherwise.
        """
        decider = self._get_decider()

        # `choose()` is executed in Rust Decider lib
        ctx = rust_decider.make_ctx(self._decider_context.to_dict())
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.warning(f"Encountered error creating Rust PyContext: {ctx_err}")

        choice = decider.choose(experiment_name, ctx)
        error = choice.err()
        variant = choice.decision()

        if error:
            logger.warning(f"Encountered error in Rust Decider: {error}")
            return None
        else:
            pass
            # todo: implement expose (requires rust updates)
            # context_fields = self._decider_context.to_dict()
            # inputs = context_fields.update(exposure_kwargs or {})
            # for event in choice.events:
            #     decider event:
            #     “experiment_id:experiment_name:experiment_version:variant_name:bucket_val:start_ts:stop_ts:owner:event_type"
            #     id, name, version, variant, bucket_val, start_ts, stop_ts, owner, event_type = event.split(“:”)
            #     experiment = ExperimentConfig(
            #         id=id,
            #         name=name,
            #         version=version,
            #         variant=variant,
            #         bucket_val=bucket_val,
            #         start_ts=start_ts,
            #         stop_ts=stop_ts,
            #         owner=owner
            #     )
            #
            #     # make work with these fields
            #     # https://github.snooguts.net/reddit/reddit-service-graphql/blob/3c5b239755b8ffb5770bfaa5ef5f5fd9e5e10635/graphql-py/graphql_api/events/utils.py#L218-L244
            #     self._event_logger.log(
            #         experiment=experiment,
            #         variant=variant,
            #         span=self._span,
            #         event_type=EVENT_TYPE,
            #         inputs=inputs,
            #         **context_fields,
            #     )

        return variant

    # todo:
    # def get_variant_without_expose(self, experiment_name: str) -> Optional[str]:

    # todo:
    # def expose(
    #     self, experiment_name: str, variant_name: str, **exposure_kwargs: Optional[Dict[str, Any]]
    # ) -> None:


class DeciderContextFactory(ContextFactory):
    """Decider client context factory.

    This factory will attach a new
    :py:class:`reddit_decider.Decider` to an attribute on the
    :py:class:`~baseplate.RequestContext`.

    :param path: Path to the experiment configuration file.
    :param event_logger: The logger to use to log experiment eligibility
        events. If not provided, a :py:class:`~baseplate.lib.events.DebugLogger`
        will be created and used.
    :param timeout: How long, in seconds, to block instantiation waiting
        for the watched experiments file to become available (defaults to not
        blocking).
    :param backoff: retry backoff time for experiments file watcher. Defaults to
        None, which is mapped to DEFAULT_FILEWATCHER_BACKOFF.
    :param request_field_extractor: an optional function used to populate
        "app_name" & "build_number" fields in DeciderContext()

    """
    def __init__(
        self,
        path: str,
        event_logger: Optional[EventLogger] = None,
        timeout: Optional[float] = None,
        backoff: Optional[float] = None,
        request_field_extractor: Callable[[BaseplateRequest], Dict[str, str]] = None
    ):
        self._filewatcher = FileWatcher(path=path, parser=init_decider_parser, timeout=timeout, backoff=backoff)
        self._event_logger = event_logger
        self._request_field_extractor = request_field_extractor

    @classmethod
    def is_employee(cls, edge_context: Any) -> bool:
        return (
            any([edge_context.user.has_role(role) for role in EMPLOYEE_ROLES])
            if edge_context.user.is_logged_in
            else False
        )

    def make_object_for_context(self, name: str, span: Span) -> Decider:
        try:
            decider = self._filewatcher.get_data()
        except WatchedFileNotAvailableError as exc:
            logger.error("Experiment config file unavailable: %s", str(exc))
        except TypeError as exc:
            logger.error("Could not load experiment config: %s", str(exc))

        validate_decider(decider)

        try:
            request = span.context
            ec = request.edgecontext

            if self._request_field_extractor:
                extracted_fields = self._request_field_extractor(request)
            else:
                extracted_fields = {}

            user_event_fields = ec.user.event_fields()

            decider_context = DeciderContext(
                user_id=user_event_fields.get("user_id"),
                logged_in=user_event_fields.get("logged_in"),
                country_code=ec.geolocation.country_code,
                locale=ec.locale.locale_code,
                origin_service=ec.origin_service.name,
                user_is_employee=DeciderContextFactory.is_employee(ec),
                device_id=ec.device.id,
                request_url=request.request_url,
                authentication_token=ec.authentication_token,
                app_name=extracted_fields.get("app_name"),
                build_number=extracted_fields.get("build_number"),
                cookie_created_timestamp=user_event_fields.get("cookie_created_timestamp"),
            )
        except Exception as exc:
            logger.warning("Could not create full DeciderContext(): %s", str(exc))
            logger.warning("defaulting to empty DeciderContext().")
            decider_context = DeciderContext(user_id="")

        return Decider(
            decider_context=decider_context,
            config_watcher=self._filewatcher,
            server_span=span,
            context_name=name,
            event_logger=self._event_logger,
        )


def decider_client_from_config(
    app_config: config.RawConfig,
    event_logger: EventLogger,
    prefix: str = "experiments.",
    request_field_extractor: Callable[[BaseplateRequest], Dict[str, str]] = None
) -> DeciderContextFactory:
    """Configure and return an :py:class:`DeciderContextFactory` object.

    The keys useful to :py:func:`decider_client_from_config` should be prefixed, e.g.
    ``experiments.path``, etc.

    Supported keys:

    ``path`` (optional)
        The path to the experiment configuration file generated by the
        experiment configuration fetcher daemon.
    ``timeout`` (optional)
        The time that we should wait for the file specified by ``path`` to
        exist.  Defaults to `None` which is `infinite`.
    ``backoff`` (optional)
        The base amount of time for exponential backoff when trying to find the
        experiments config file. Defaults to no backoff between tries.
    ``request_field_extractor`` (optional) used to populate
        "app_name" & "build_number" fields in DeciderContext()

    :param raw_config: The application configuration which should have
        settings for the experiments client.
    :param event_logger: The EventLogger to be used to log bucketing events.
    :param prefix: the prefix used to filter keys (defaults to "experiments.").

    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]

    cfg = config.parse_config(
        app_config,
        {
            config_prefix: {
                "path": config.Optional(config.String, default="/var/local/experiments.json"),
                "timeout": config.Optional(config.Timespan),
                "backoff": config.Optional(config.Timespan),
            }
        },
    )
    options = getattr(cfg, config_prefix)

    # pylint: disable=maybe-no-member
    if options.timeout:
        timeout = options.timeout.total_seconds()
    else:
        timeout = None

    if options.backoff:
        backoff = options.backoff.total_seconds()
    else:
        backoff = None

    return DeciderContextFactory(
        path=options.path,
        event_logger=event_logger,
        timeout=timeout,
        backoff=backoff,
        request_field_extractor=request_field_extractor
    )
