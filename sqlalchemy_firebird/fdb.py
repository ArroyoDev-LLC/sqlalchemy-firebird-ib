"""
.. dialect:: firebird+fdb
    :name: fdb
    :dbapi: pyodbc
    :connectstring: firebird+fdb://user:password@host:port/path/to/db[?key=value&key=value...]
    :url: http://pypi.python.org/pypi/fdb/

    fdb is a kinterbasdb compatible DBAPI for Firebird.

    .. versionchanged:: 0.9 - The fdb dialect is now the default dialect
       under the ``firebird://`` URL space, as ``fdb`` is now the official
       Python driver for Firebird.

Arguments
----------

The ``fdb`` dialect is based on the
:mod:`sqlalchemy.dialects.firebird.kinterbasdb` dialect, however does not
accept every argument that Kinterbasdb does.

* ``enable_rowcount`` - True by default, setting this to False disables
  the usage of "cursor.rowcount" with the
  Kinterbasdb dialect, which SQLAlchemy ordinarily calls upon automatically
  after any UPDATE or DELETE statement.   When disabled, SQLAlchemy's
  ResultProxy will return -1 for result.rowcount.   The rationale here is
  that Kinterbasdb requires a second round trip to the database when
  .rowcount is called -  since SQLA's resultproxy automatically closes
  the cursor after a non-result-returning statement, rowcount must be
  called, if at all, before the result object is returned.   Additionally,
  cursor.rowcount may not return correct results with older versions
  of Firebird, and setting this flag to False will also cause the
  SQLAlchemy ORM to ignore its usage. The behavior can also be controlled on a
  per-execution basis using the ``enable_rowcount`` option with
  :meth:`.Connection.execution_options`::

      conn = engine.connect().execution_options(enable_rowcount=True)
      r = conn.execute(stmt)
      print r.rowcount

* ``retaining`` - False by default.   Setting this to True will pass the
  ``retaining=True`` keyword argument to the ``.commit()`` and ``.rollback()``
  methods of the DBAPI connection, which can improve performance in some
  situations, but apparently with significant caveats.
  Please read the fdb and/or kinterbasdb DBAPI documentation in order to
  understand the implications of this flag.

  .. versionchanged:: 0.9.0 - the ``retaining`` flag defaults to ``False``.
     In 0.8 it defaulted to ``True``.

  .. seealso::

    http://pythonhosted.org/fdb/usage-guide.html#retaining-transactions
    - information on the "retaining" flag.

"""  # noqa

from .kinterbasdb import FBDialect_kinterbasdb
from sqlalchemy import util
from sqlalchemy import LargeBinary
from sqlalchemy import types as sqltypes


class _FBBlob(LargeBinary):
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                try:
                    value = bytes(value)
                except TypeError:
                    value.close()
                    return None
            return value

        return process


class FBDialect_fdb(FBDialect_kinterbasdb):
    driver = "fdb"
    supports_statement_cache = True
    is_interbase = False
    colspecs = util.update_copy(
        FBDialect_kinterbasdb.colspecs, {sqltypes.LargeBinary: _FBBlob}
    )

    def __init__(
        self,
        enable_rowcount=True,
        retaining=False,
        is_interbase=False,
        **kwargs,
    ):
        self.is_interbase = is_interbase
        super(FBDialect_fdb, self).__init__(
            enable_rowcount=enable_rowcount, retaining=retaining, **kwargs
        )

    @classmethod
    def dbapi(cls):
        return __import__("fdb")

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user")
        if opts.get("port"):
            opts["host"] = "%s/%s" % (opts["host"], opts["port"])
            del opts["port"]
        opts.update(url.query)

        util.coerce_kw_type(opts, "type_conv", int)

        return ([], opts)

    def _get_server_version_info(self, connection):
        """Get the version of the Firebird server used by a connection.

        Returns a tuple of (`major`, `minor`, `build`), three integers
        representing the version of the attached server.
        """
        # This is the simpler approach (the other uses the services api),
        # that for backward compatibility reasons returns a string like
        #   LI-V6.3.3.12981 Firebird 2.0
        # where the first version is a fake one resembling the old
        # Interbase signature.
        isc_info_firebird_version = 103 if not self.is_interbase else 12
        fbconn = connection.connection

        try:
            version = fbconn.db_info(isc_info_firebird_version)
        except Exception as e:
            try:
                version = fbconn.db_info(
                    12 if isc_info_firebird_version == 103 else 103
                )
            except Exception as ee:
                raise ee from e

        return self._parse_version_info(version)


dialect = FBDialect_fdb
