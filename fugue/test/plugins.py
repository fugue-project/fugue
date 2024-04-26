from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type
from fugue.dataframe.utils import _df_eq
from triad import assert_or_throw, run_once
from triad.utils.entry_points import load_entry_point

try:
    import pytest

    _HAS_PYTEST = True
except ImportError:  # pragma: no cover
    _HAS_PYTEST = False


_FUGUE_TEST_BACKENDS: Dict[str, Type["FugueTestBackend"]] = {}
_FUGUE_TEST_ALL_INI_CONF: Dict[str, Any] = {}
_FUGUE_TEST_INI_FUGUE_CONF: Dict[str, Any] = {}


def _set_global_conf(conf: Dict[str, Any]) -> None:
    global _FUGUE_TEST_ALL_INI_CONF, _FUGUE_TEST_INI_FUGUE_CONF
    _FUGUE_TEST_ALL_INI_CONF = conf
    _FUGUE_TEST_INI_FUGUE_CONF = _extract_fugue_conf(conf)


def _get_all_ini_conf() -> Dict[str, Any]:
    return _FUGUE_TEST_ALL_INI_CONF


@run_once
def _load_all_backends() -> None:
    from fugue.constants import FUGUE_ENTRYPOINT

    load_entry_point(FUGUE_ENTRYPOINT)


def with_backend(ctx: Any, *other: Any, skip_missing: bool = False) -> Any:
    """The decorator to set the backend context for the test function

    :param ctx: the object to specify the backend
    :param other: more objects to specify the additional backends
    :param skip_missing: skip if an object can't be converted to a backend,
        defaults to False

    .. admonition:: Examples

        .. code-block:: python

            import fugue.test as ft
            import fugue.api as fa

            @ft.with_backend("pandas", "spark")
            def test_spark(path_fixture):
                # test load and count under spark and pandas
                df = fa.load(path_fixture)
                assert fa.count(df) == 3

    .. note::

        ``ctx`` can be a string or a tuple of string and dict. If it contains a
        dict, the dict will be merged with the default configuration for the backend.

        the configuration of each backend is extracted from the pytest ini configuration
        under ``fugue_test_conf``. The configuration must prefixed with the backend
        name.

        ``fugue_test_conf`` is defined at the parallel place where you define pytest
        ``addopts``. For example, if you define them in ``setup.cfg``, then it
        looks like:

        .. code-block:: ini

            [tool:pytest]
            addopts =
                -p pytest_cov
                --cov=fugue
            fugue_test_conf =
                fugue.test.dummy=dummy
                fugue.test:bool=true
                # ray backend settings
                ray.num_cpus:int=2
                # dask backend settings
                dask.processes:bool=true
                dask.n_workers:int=3
                dask.threads_per_worker:int=1
    """
    import pytest

    _load_all_backends()

    _ctx = _construct_parameterized_fixture([ctx] + list(other), skip_missing)
    return lambda f: pytest.mark.parametrize("backend_context", _ctx, indirect=True)(
        pytest.mark.usefixtures("backend_context")(f)
    )


def fugue_test_backend(cls: Type["FugueTestBackend"]) -> Type["FugueTestBackend"]:
    """The decorator to register a Fugue test backend. Most Fugue users don't need
    to use this decorator.

    :param cls: the FugueTestBackend class

    .. admonition:: Examples

        .. code-block:: python

            import fugue.test as ft

            @ft.fugue_test_backend
            class MyBackend(ft.FugueTestBackend):
                name = "my_backend"  # the name of the backend

                @classmethod
                @contextmanager
                def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
                    # create a session object
                    yield session

    """
    assert_or_throw(
        issubclass(cls, FugueTestBackend),
        ValueError(f"{cls} is not a FugueTestBackend"),
    )
    name = cls.name.strip().lower()
    assert_or_throw(
        name != "" and name != "fugue",
        ValueError(f"Fugue test backend name cannot be empty or fugue: {cls}"),
    )
    assert_or_throw(
        name not in _FUGUE_TEST_BACKENDS,
        ValueError(f"Duplicate Fugue test backend name: {name}"),
    )
    _FUGUE_TEST_BACKENDS[name] = cls
    return cls


class FugueTestSuite:
    """The base class for Fugue test suite. Most Fugue users don't need to use this
    class or the decorator :func:`~.fugue_test_suite`. The dccorator and the class
    are often used together.

    .. admonition:: Examples

        .. code-block:: python

            import fugue.test as ft

            @ft.fugue_test_suite("pandas")
            class MyTest(ft.FugueTestSuite):
                def test_pandas(self):
                    # test pandas
                    pass

                def test_spark(self):
                    # test spark
                    pass
    """

    backend: Any
    tmp_path: Path
    equal_type_groups: Any = None

    __test__ = False
    _test_context: Any = None

    if _HAS_PYTEST:

        @pytest.fixture(autouse=True)
        def init_builtin_per_func_context(self, tmp_path):
            self.tmp_path = tmp_path

    @property
    def context(self) -> "FugueTestContext":
        """The ``FugueTestContext`` object"""
        return self._test_context

    @property
    def engine(self) -> Any:
        """The engine object inside the ``FugueTestContext``"""
        return self.context.engine

    def get_equal_type_groups(self) -> Optional[List[List[Any]]]:
        return None  # pragma: no cover

    def df_eq(self, *args: Any, **kwargs: Any) -> bool:
        """A wrapper of :func:`~fugue.dataframe.utils.df_eq`"""
        if "equal_type_groups" not in kwargs:
            kwargs["equal_type_groups"] = self.equal_type_groups
        return _df_eq(*args, **kwargs)


def fugue_test_suite(backend: Any, mark_test: Optional[bool] = None) -> Any:
    def deco(cls: Type["FugueTestSuite"]) -> Type["FugueTestSuite"]:
        import pytest

        assert_or_throw(
            issubclass(cls, FugueTestSuite),
            ValueError(f"{cls} is not a FugueTestSuite"),
        )
        if mark_test is not None:
            cls.__test__ = mark_test
        c, extra_conf = _parse_backend(backend)
        return pytest.mark.parametrize(
            "backend_context", [pytest.param((c, extra_conf), id=c)], indirect=True
        )(pytest.mark.usefixtures("_class_backend_context")(cls))

    return deco


@dataclass
class FugueTestContext:
    """The context object for Fugue test

    :param engine: the Fugue ExecutionEngine object
    :param session: the original session wrapped by the Fugue ExecutionEngine
    :param name: the backend name
    """

    engine: Any
    session: Any
    name: str


class FugueTestBackend:
    """The base class for Fugue test backend. Most Fugue users don't need to use this
    class or the decorator :func:`~.fugue_test_backend`. The dccorator and the class
    are often used together.

    .. admonition:: Examples

        .. code-block:: python

            import fugue.test as ft

            @ft.fugue_test_backend
            class MyBackend(ft.FugueTestBackend):
                name = "my_backend"  # the name of the backend

                @classmethod
                @contextmanager
                def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
                    # create a session object
                    yield session
    """

    name = ""
    default_session_conf: Dict[str, Any] = {}
    default_fugue_conf: Dict[str, Any] = {}
    session_conf: Dict[str, Any] = {}
    fugue_conf: Dict[str, Any] = {}

    @classmethod
    def transform_session_conf(cls, conf: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and transform the ``conf`` and keep the ones specific to
        this backend. The default implementation will extract the configuration
        prefixed with ``<backend name>.`` and remove the prefix.

        :param conf: the raw configuration
        :return: the transformed configuration
        """
        return extract_conf(conf, cls.name + ".", remove_prefix=True)

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        """Create the backend native session object

        :param session_conf: the configuration for this backend
        :yield: the session object
        """
        raise NotImplementedError  # pragma: no cover

    @classmethod
    @contextmanager
    def generate_session_fixture(cls) -> Iterator[Any]:
        """Generate the session fixture for this backend from the default backend
        configuration and then the pytest ini configuration under ``fugue_test_conf``

        :yield: the session object
        """
        session_conf = _merge_dicts(
            cls.default_session_conf,
            cls.transform_session_conf(_FUGUE_TEST_ALL_INI_CONF),
        )
        with cls.session_context(session_conf) as session:
            yield session

    @classmethod
    @contextmanager
    def generate_context_fixture(
        cls, session: object, extra_fugue_conf: Dict[str, Any]
    ) -> Iterator[FugueTestContext]:
        """Generate the ``FugueTestContext`` fixture for this backend from the
        default backend configuration and then the pytest ini configuration under
        ``fugue_test_conf`` and then ``extra_fugue_conf``.

        :param session: the session object
        :param extra_fugue_conf: the extra fugue configuration

        :yield: the ``FugueTestContext`` object
        """
        import fugue.api as fa

        fugue_conf = _merge_dicts(
            cls.default_fugue_conf,
            _FUGUE_TEST_INI_FUGUE_CONF,
            _extract_fugue_conf(extra_fugue_conf),
        )
        with fa.engine_context(session, fugue_conf) as engine:
            yield FugueTestContext(engine=engine, session=session, name=cls.name)


def extract_conf(
    conf: Dict[str, Any], prefix: str, remove_prefix: bool
) -> Dict[str, Any]:
    """Extract the configuration prefixed with ``prefix`` and optionally remove the
    prefix

    :param conf: the raw configuration
    :param prefix: the prefix
    :param remove_prefix: whether to remove the prefix

    :return: the extracted configuration
    """
    res: Dict[str, Any] = {}
    for k, v in conf.items():
        if k.startswith(prefix):
            if remove_prefix:
                k = k[len(prefix) :]
            res[k] = v
    return res


@contextmanager
def _make_backend_context(obj: Any, session: Any) -> Iterator[Any]:
    _load_all_backends()
    key, extra_conf = _parse_backend(obj)
    assert_or_throw(
        key in _FUGUE_TEST_BACKENDS,
        lambda: ValueError(
            f"Undefined Fugue test backend: {key}, "
            f"available backends: {list(_FUGUE_TEST_BACKENDS.keys())}"
        ),
    )
    with _FUGUE_TEST_BACKENDS[key].generate_context_fixture(session, extra_conf) as ctx:
        yield ctx


def _extract_fugue_conf(conf: Dict[str, Any]) -> Dict[str, Any]:
    return extract_conf(conf, "fugue.", remove_prefix=False)


def _construct_parameterized_fixture(ctx: List[Any], skip_missing: bool) -> List[Any]:
    import pytest

    _ctx: List[Tuple[str, Dict[str, Any]]] = []
    for x in ctx:
        c, extra_conf = _parse_backend(x)
        if c not in _FUGUE_TEST_BACKENDS:
            if not skip_missing:
                raise ValueError(
                    f"Undefined Fugue test backend: {c}, "
                    f"available backends: {list(_FUGUE_TEST_BACKENDS.keys())}"
                )
            else:
                _ctx.append(
                    pytest.param(
                        c,
                        marks=pytest.mark.xfail(
                            reason="Undefined Fugue test backend", run=False
                        ),
                        id=c,
                    )
                )
        else:
            _ctx.append(pytest.param((c, extra_conf), id=c))
    return _ctx


def _merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    for d in dicts:
        res.update(d)
    return res


def _parse_backend(ctx: Any) -> Tuple[str, Dict[str, Any]]:
    if isinstance(ctx, str):
        return ctx, {}
    else:
        return ctx[0], ctx[1]
