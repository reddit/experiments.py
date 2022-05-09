import logging

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, IO, Optional

from baseplate import RequestContext
from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.events import DebugLogger
from baseplate.lib.events import EventLogger
from baseplate.lib.file_watcher import FileWatcher
from baseplate.lib.file_watcher import T
from baseplate.lib.file_watcher import WatchedFileNotAvailableError
from reddit_edgecontext import ValidatedAuthenticationToken

import rust_decider


logger = logging.getLogger(__name__)

EMPLOYEE_ROLES = ("employee", "contractor")

class EventType(Enum):
    EXPOSE = "expose"

@dataclass
class ExperimentConfig:
    id: int
    version: str
    name: str
    bucket_val: str
    start_ts: int
    stop_ts: int
    owner: str

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
        auth_client_id: Optional[str] = None,
        origin_service: Optional[str] = None,
        cookie_created_timestamp: Optional[float] = None,
        extracted_fields: Optional[dict] = None
    ):
        self._user_id = user_id
        self._country_code = country_code
        self._locale = locale
        self._user_is_employee = user_is_employee
        self._logged_in = logged_in
        self._device_id = device_id
        self._auth_client_id = auth_client_id
        self._origin_service = origin_service
        self._cookie_created_timestamp = cookie_created_timestamp
        self._extracted_fields = extracted_fields

    def to_dict(self) -> Dict:
        ef = (self._extracted_fields or {}).copy()

        return {
            "user_id": self._user_id,
            "country_code": self._country_code,
            "locale": self._locale,
            "user_is_employee": self._user_is_employee,
            "logged_in": self._logged_in,
            "device_id": self._device_id,
            "auth_client_id": self._auth_client_id,
            "origin_service": self._origin_service,
            "cookie_created_timestamp": self._cookie_created_timestamp,
            "other_fields": ef,
            **ef,
        }

    def to_event_dict(self) -> Dict:
        user_fields = {
            "id": self._user_id,
            "logged_in": self._logged_in,
            "cookie_created_timestamp": self._cookie_created_timestamp,
            "is_employee": self._user_is_employee
        }

        ef = (self._extracted_fields or {}).copy()

        app_fields = {}
        if ef.get("app_name"):
            app_fields["name"] = ef["app_name"]
        if ef.get("build_number"):
            app_fields["build_number"] = ef["build_number"]
        if self._locale:
            app_fields["relevant_locale"] = self._locale

        geo_fields = {}
        if self._country_code:
            geo_fields["country_code"] = self._country_code

        request_fields = {}
        if ef.get("canonical_url"):
            request_fields["canonical_url"] = ef["canonical_url"]

        platform_fields = {}
        if self._device_id:
            platform_fields["device_id"] = self._device_id

        return {
            "user_id": self._user_id,
            "country_code": self._country_code,
            "locale": self._locale,
            "user_is_employee": self._user_is_employee,
            "logged_in": self._logged_in,
            "device_id": self._device_id,
            "origin_service": self._origin_service,
            "cookie_created_timestamp": self._cookie_created_timestamp,
            "user": user_fields,
            "app": app_fields,
            "geo": geo_fields,
            "request": request_fields,
            "platform": platform_fields,
            **ef,
        }

def init_decider_parser(file: IO) -> Any:
    return rust_decider.init("darkmode overrides targeting holdout mutex_group fractional_availability value", file.name)

def validate_decider(decider: Optional[Any]) -> None:
    if decider is None:
        logger.error("Rust decider is None--did not initialize.")

    if decider:
        decider_err = decider.err()
        if decider_err:
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

    def _get_decider(self) -> Optional[T]:
        try:
            decider = self._config_watcher.get_data()
            validate_decider(decider)
            return decider
        except WatchedFileNotAvailableError as exc:
            logger.error("Experiment config file unavailable: %s", str(exc))
        except TypeError as exc:
            logger.error("Could not load experiment config: %s", str(exc))
        return None

    def get_variant(
        self,
        experiment_name: str,
        **exposure_kwargs: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        """Return a bucketing variant, if any, with auto-exposure.

        Since calling `get_variant()` will fire an exposure event, it
        is best to call it when you are sure the user will be exposed to the experiment.
        If you absolutely must check the status of an experiment
        before the user will be exposed to the experiment,
        use `get_variant_without_expose()` to disable exposure events
        and call `expose()` manually later.

        :param experiment_name: Name of the experiment you want a variant for.

        :param exposure_kwargs:  Additional arguments that will be passed
            to events_logger (keys must be part of v2 event schema).

        :return: Variant name if a variant is assigned, None otherwise.
        """
        decider = self._get_decider()
        if decider is None:
            logger.warning("Encountered error in _get_decider()")
            return None

        context_fields = self._decider_context.to_dict()
        ctx = rust_decider.make_ctx(context_fields)
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.warning(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")

        choice = decider.choose(experiment_name, ctx)
        error = choice.err()

        if error:
            logger.warning(f"Encountered error in decider.choose(): {error}")
            return None

        variant = choice.decision()

        event_context_fields = self._decider_context.to_event_dict()
        event_context_fields.update(exposure_kwargs or {})

        for event in choice.events():
            _event_type, exp_id, name, version, event_variant, _bucketing_value, bucket_val, start_ts, stop_ts, owner = event.split(":")
            experiment = ExperimentConfig(
                id=int(exp_id),
                name=name,
                version=version,
                bucket_val=bucket_val,
                start_ts=start_ts,
                stop_ts=stop_ts,
                owner=owner
            )

            self._event_logger.log(
                experiment=experiment,
                variant=event_variant,
                span=self._span,
                event_type=EventType.EXPOSE,
                inputs=event_context_fields.copy(),
                **event_context_fields.copy(),
            )

        return variant

    def get_variant_without_expose(self, experiment_name: str) -> Optional[str]:
        """Return a bucketing variant, if any, without emitting exposure event.

        The `expose()` function is available to be manually called afterward.

        However, experiments in Holdout Groups will still send an exposure for
        the holdout parent experiment, since it is not possible to
        manually expose the holdout later (because after exiting this function,
        it's impossible to know if a returned `None` or `"control_1"` string
        came from the holdout group or its child experiment).

        :param experiment_name: Name of the experiment you want a variant for.

        :return: Variant name if a variant is assigned, None otherwise.
        """
        decider = self._get_decider()
        if decider is None:
            logger.warning("Encountered error in _get_decider()")
            return None

        context_fields = self._decider_context.to_dict()
        ctx = rust_decider.make_ctx(context_fields)
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.warning(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")

        choice = decider.choose(experiment_name, ctx)
        error = choice.err()

        if error:
            logger.warning(f"Encountered error in decider.choose(): {error}")
            return None

        variant = choice.decision()

        event_context_fields = self._decider_context.to_event_dict()

        # expose Holdout if the experiment is part of one
        for event in choice.events():
            event_type, exp_id, name, version, event_variant, _bucketing_value, bucket_val, start_ts, stop_ts, owner = event.split(":")
            # event_type enum:
            #   0: regular bucketing
            #   1: override
            #   2: holdout
            if event_type == "2":
                experiment = ExperimentConfig(
                    id=int(exp_id),
                    name=name,
                    version=version,
                    bucket_val=bucket_val,
                    start_ts=start_ts,
                    stop_ts=stop_ts,
                    owner=owner
                )

                self._event_logger.log(
                    experiment=experiment,
                    variant=event_variant,
                    span=self._span,
                    event_type=EventType.EXPOSE,
                    inputs=event_context_fields.copy(),
                    **event_context_fields.copy(),
                )

        return variant

    def expose(
        self, experiment_name: str, variant_name: str, **exposure_kwargs: Optional[Dict[str, Any]]
    ) -> None:
        """Log an event to indicate that a user has been exposed to an experimental treatment.

        Meant to be used after calling `get_variant_without_expose()`
        since `get_variant()` emits exposure event automatically.

        :param experiment_name: Name of the experiment that was exposed.

        :param variant_name: Name of the variant that was exposed.

        :param exposure_kwargs: Additional arguments that will be passed
            to events_logger (keys must be part of v2 event schema).
        """
        decider = self._get_decider()
        if decider is None:
            logger.warning("Encountered error in _get_decider()")
            return

        experiment = decider.get_experiment(experiment_name)
        error = experiment.err()
        if error:
            logger.warning(f"Encountered error in decider.get_experiment(): {error}")
            return

        event_context_fields = self._decider_context.to_event_dict()
        event_context_fields.update(exposure_kwargs or {})
        exp_dict = experiment.val()

        experiment = ExperimentConfig(
            id=int(exp_dict.get("id", 0)),
            name=exp_dict.get("name"),
            version=str(exp_dict.get("version")),
            bucket_val=exp_dict.get("variant_set", {}).get("bucket_val"),
            start_ts=exp_dict.get("variant_set", {}).get("start_ts"),
            stop_ts=exp_dict.get("variant_set", {}).get("stop_ts"),
            owner=exp_dict.get("owner")
        )

        self._event_logger.log(
            experiment=experiment,
            variant=variant_name,
            span=self._span,
            event_type=EventType.EXPOSE,
            inputs=event_context_fields.copy(),
            **event_context_fields.copy(),
        )

    def get_variant_for_identifier(
        self,
        experiment_name: str,
        identifier: str,
        **exposure_kwargs: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        """Return a bucketing variant for `identifier`, if any, with auto-exposure.

        The `identifier` param will be set on `DeciderClient` under:
            `user_id`, `device_id`, & `canonical_url`,
        so that regardless of what `bucketing_val` is set to in an experiment's config
        (one of those 3), the passed in `identifier` param will be used in the
        hashing string when bucketing.

        Since calling `get_variant_for_identifier()` will fire an exposure event, it
        is best to call it when you are sure the user will be exposed to the experiment.

        :param experiment_name: Name of the experiment you want a variant for.

        :param identifier: an arbitary string used to bucket the experiment by
            being set on DeciderContext's 3 possible `bucket_val`'s
            (`user_id`, `device_id`, & `canonical_url`).

        :param exposure_kwargs:  Additional arguments that will be passed
            to events_logger under "inputs" key.

        :return: Variant name if a variant is assigned, None otherwise.
        """
        decider = self._get_decider()
        if decider is None:
            logger.warning("Encountered error in _get_decider()")
            return None

        identifier_context_fields = { **self._decider_context.to_dict(), **{
            "user_id": identifier,
            "device_id": identifier,
            "canonical_url": identifier
        }}

        ctx = rust_decider.make_ctx(identifier_context_fields)
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.warning(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")

        choice = decider.choose(experiment_name, ctx)
        error = choice.err()

        if error:
            logger.warning(f"Encountered error in decider.choose(): {error}")
            return None

        variant = choice.decision()

        event_context_fields = self._decider_context.to_event_dict()
        event_context_fields.update(exposure_kwargs or {})

        for event in choice.events():
            _event_type, exp_id, name, version, event_variant, bucketing_value, bucket_val, start_ts, stop_ts, owner = event.split(":")
            experiment = ExperimentConfig(
                id=int(exp_id),
                name=name,
                version=version,
                bucket_val=bucket_val,
                start_ts=start_ts,
                stop_ts=stop_ts,
                owner=owner
            )

            # expose the `bucket_val` used in the experiment's config, not `identifier_context_fields`
            event_ctx_fields = { **event_context_fields, **{bucket_val: bucketing_value}}

            self._event_logger.log(
                experiment=experiment,
                variant=event_variant,
                span=self._span,
                event_type=EventType.EXPOSE,
                inputs=event_ctx_fields.copy(),
                **event_ctx_fields.copy(),
            )

        return variant


    def get_variant_for_identifier_without_expose(
        self,
        experiment_name: str,
        identifier: str,
    ) -> Optional[str]:
        """Return a bucketing variant for `identifier`, if any, without emitting exposure event.

        The `identifier` param will be set on `DeciderClient` under:
            `user_id`, `device_id`, & `canonical_url`,
        so that regardless of what `bucketing_val` is set to in an experiment's config
        (one of those 3), the passed in `identifier` param will be used in the
        hashing string when bucketing.

        The `expose()` function is available to be manually called afterward to emit
        exposure event.

        However, experiments in Holdout Groups will still send an exposure for
        the holdout parent experiment, since it is not possible to
        manually expose the holdout later (because after exiting this function,
        it's impossible to know if a returned `None` or `"control_1"` string
        came from the holdout group or its child experiment).

        :param experiment_name: Name of the experiment you want a variant for.

        :param identifier: an arbitary string used to bucket the experiment by
            being set on DeciderContext's 3 possible `bucket_val`'s
            (`user_id`, `device_id`, & `canonical_url`).

        :return: Variant name if a variant is assigned, None otherwise.
        """
        decider = self._get_decider()
        if decider is None:
            logger.warning("Encountered error in _get_decider()")
            return None

        identifier_context_fields = { **self._decider_context.to_dict(), **{
            "user_id": identifier,
            "device_id": identifier,
            "canonical_url": identifier
        }}

        ctx = rust_decider.make_ctx(identifier_context_fields)
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.warning(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")

        choice = decider.choose(experiment_name, ctx)
        error = choice.err()

        if error:
            logger.warning(f"Encountered error in decider.choose(): {error}")
            return None

        variant = choice.decision()

        event_context_fields = self._decider_context.to_event_dict()

        # expose Holdout if the experiment is part of one
        for event in choice.events():
            event_type, exp_id, name, version, event_variant, bucketing_value, bucket_val, start_ts, stop_ts, owner = event.split(":")
            # event_type enum:
            #   0: regular bucketing
            #   1: override
            #   2: holdout
            if event_type == "2":
                experiment = ExperimentConfig(
                    id=int(exp_id),
                    name=name,
                    version=version,
                    bucket_val=bucket_val,
                    start_ts=start_ts,
                    stop_ts=stop_ts,
                    owner=owner
                )

                # expose the `bucket_val` used in the experiment's config, not `identifier_context_fields`
                event_ctx_fields = { **event_context_fields, **{bucket_val: bucketing_value}}

                self._event_logger.log(
                    experiment=experiment,
                    variant=event_variant,
                    span=self._span,
                    event_type=EventType.EXPOSE,
                    inputs=event_ctx_fields.copy(),
                    **event_ctx_fields.copy(),
                )

        return variant

    def _get_dynamic_config_value(
        self,
        feature_name: str,
        decider_func: Callable[[str, DeciderContext], any],
        default: any,
    ) -> Optional[any]:
        context_fields = self._decider_context.to_dict()
        ctx = rust_decider.make_ctx(context_fields)
        ctx_err = ctx.err()
        if ctx_err is not None:
            logger.warning(f"Encountered error in rust_decider.make_ctx(): {ctx_err}")

        res = decider_func(feature_name, ctx)
        if res is None:
            return default
        error = res.err()
        if error:
            logger.warning(f"Encountered error {decider_func.__name__}: {error}")
            return default

        return res.val()

    def get_bool(self, feature_name: str, default: bool = False) -> bool:
        decider = self._get_decider()
        if not decider:
            logger.warning("Encountered error in _get_decider()")
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_bool, default)

    def get_int(self, feature_name: str, default: int = 0) -> int:
        decider = self._get_decider()
        if not decider:
            logger.warning("Encountered error in _get_decider()")
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_int, default)

    def get_float(self, feature_name: str, default: float = 0.0) -> float:
        decider = self._get_decider()
        if not decider:
            logger.warning("Encountered error in _get_decider()")
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_float, default)

    def get_string(self, feature_name: str, default: str = "") -> str:
        decider = self._get_decider()
        if not decider:
            logger.warning("Encountered error in _get_decider()")
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_string, default)

    def get_map(self, feature_name: str, default: dict = None) -> Optional[dict]:
        decider = self._get_decider()
        if not decider:
            logger.warning("Encountered error in _get_decider()")
            return default
        return self._get_dynamic_config_value(feature_name, decider.get_map, default)


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
    :param request_field_extractor: an optional function used to populate fields such as
        "app_name" & "build_number" in DeciderContext() via `extracted_fields` arg

    """
    def __init__(
        self,
        path: str,
        event_logger: Optional[EventLogger] = None,
        timeout: Optional[float] = None,
        backoff: Optional[float] = None,
        request_field_extractor: Optional[Callable[[RequestContext], Dict[str, str]]] = None
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
        decider = None
        try:
            decider = self._filewatcher.get_data()
        except WatchedFileNotAvailableError as exc:
            logger.error("Experiment config file unavailable: %s", str(exc))
        except TypeError as exc:
            logger.error("Could not load experiment config: %s", str(exc))

        validate_decider(decider)

        try:
            request = span.context
            ec = request.edge_context

            if self._request_field_extractor:
                extracted_fields = self._request_field_extractor(request)
            else:
                extracted_fields = {}

            user_event_fields = ec.user.event_fields()

            auth_client_id = ""
            if isinstance(ec.authentication_token, ValidatedAuthenticationToken):
                oc_id = ec.authentication_token.oauth_client_id
                if oc_id:
                    auth_client_id = oc_id

            decider_context = DeciderContext(
                user_id=user_event_fields.get("user_id"),
                logged_in=user_event_fields.get("logged_in"),
                country_code=ec.geolocation.country_code,
                locale=ec.locale.locale_code,
                origin_service=ec.origin_service.name,
                user_is_employee=DeciderContextFactory.is_employee(ec),
                device_id=ec.device.id,
                auth_client_id=auth_client_id,
                cookie_created_timestamp=user_event_fields.get("cookie_created_timestamp"),
                extracted_fields=extracted_fields,
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
    request_field_extractor: Optional[Callable[[RequestContext], Dict[str, str]]] = None,
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
    ``request_field_extractor`` (optional) used to populate fields such as
        "app_name" & "build_number" in DeciderContext() via `extracted_fields` arg

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
        request_field_extractor=request_field_extractor,
    )
