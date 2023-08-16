# Release Notes

## 0.8.7

-   [506](https://github.com/fugue-project/fugue/issues/506) Adopt pandas `ExtensionDType`
-   [504](https://github.com/fugue-project/fugue/issues/504) Create Fugue pytest fixtures
-   [503](https://github.com/fugue-project/fugue/issues/503) Deprecate python 3.7 support
-   [501](https://github.com/fugue-project/fugue/issues/501) Simplify zip/comap, remove join from the implementation
-   [500](https://github.com/fugue-project/fugue/issues/500) Implement all partitioning strategies for Dask
-   [495](https://github.com/fugue-project/fugue/issues/495) Resolve segfault on Duckdb 0.8.1
-   [494](https://github.com/fugue-project/fugue/issues/494) Remove the version cap of Dask

## 0.8.6

-   [497](https://github.com/fugue-project/fugue/issues/497) Make LocalExecutionEngine respect partition numbers
-   [493](https://github.com/fugue-project/fugue/issues/493) Spark Pandas UDF partitioning improvement
-   [492](https://github.com/fugue-project/fugue/issues/492) Made AnyDataFrame recognized by Creator, Processor and Ouputter
-   [490](https://github.com/fugue-project/fugue/issues/490) Fixed pa.Table as transformer output bug
-   [489](https://github.com/fugue-project/fugue/issues/489) Added version cap to Ibis
-   [485](https://github.com/fugue-project/fugue/issues/485) Made Fugue compatible with Ray 2.5.0
-   [486](https://github.com/fugue-project/fugue/issues/486) Added py.typed to Fugue

## 0.8.5

-   [481](https://github.com/fugue-project/fugue/pull/481) Moved Fugue SQL dependencies into functions as soft dependencies
-   [478](https://github.com/fugue-project/fugue/issues/478) Removed cloudpickle from the hard dependency of Spark backend
-   [477](https://github.com/fugue-project/fugue/issues/477) Removed tests folder from Fugue package
-   [476](https://github.com/fugue-project/fugue/issues/476) Fix compatibility issues for Pandas 2+ and Spark < 3.4

## 0.8.4

-   [471](https://github.com/fugue-project/fugue/issues/471) Fix compatibility issues for duckdb 0.8.0+
-   [466](https://github.com/fugue-project/fugue/issues/466) Fix Ray 2.4.0 compatibility issue
-   [464](https://github.com/fugue-project/fugue/issues/464) Support for spark/databricks connect
-   [459](https://github.com/fugue-project/fugue/issues/459) DEPRECATION: Avro support
-   [455](https://github.com/fugue-project/fugue/issues/455) Make Fugue pandas 2 compatible

## 0.8.3

-   [449](https://github.com/fugue-project/fugue/issues/449) Add coarse partitioning concept
-   [452](https://github.com/fugue-project/fugue/issues/452) Add as_fugue_engine_df

## 0.8.2

-   [430](https://github.com/fugue-project/fugue/issues/430) Support Polars DataFrames
-   [434](https://github.com/fugue-project/fugue/issues/434) Make Transformations data format aware
-   [408](https://github.com/fugue-project/fugue/issues/408) Remove SQLite support
-   [444](https://github.com/fugue-project/fugue/issues/444) Clean up FunctionWrapper

## 0.8.1

-   [423](https://github.com/fugue-project/fugue/issues/423) Add seaborn as a domain level extension for visualization
-   [422](https://github.com/fugue-project/fugue/issues/422) Add pandas_df.plot as the first namespace extension
-   [421](https://github.com/fugue-project/fugue/issues/421) Add the namespace concept to Fugue extensions
-   [420](https://github.com/fugue-project/fugue/issues/420) Add is_distributed to engines
-   [419](https://github.com/fugue-project/fugue/issues/419) Log transpiled SQL query upon error

## 0.8.0

-   [384](https://github.com/fugue-project/fugue/issues/384) Expanding Fugue API
-   [410](https://github.com/fugue-project/fugue/issues/410) Unify Fugue SQL dialect (syntax only)
-   [409](https://github.com/fugue-project/fugue/issues/409) Support arbitrary column names in Fugue
-   [404](https://github.com/fugue-project/fugue/issues/404) Ray/Dask engines guess optimal default partitions
-   [403](https://github.com/fugue-project/fugue/issues/403) Deprecate register_raw_df_type
-   [392](https://github.com/fugue-project/fugue/issues/392) Aggregations on Spark dataframes fail intermittently
-   [398](https://github.com/fugue-project/fugue/issues/398) Rework API Docs and Favicon
-   [393](https://github.com/fugue-project/fugue/issues/393) ExecutionEngine as_context
-   [385](https://github.com/fugue-project/fugue/issues/385) Remove DataFrame metadata
-   [381](https://github.com/fugue-project/fugue/issues/381) Change SparkExecutionEngine to use pandas udf by default
-   [380](https://github.com/fugue-project/fugue/issues/380) Refactor ExecutionEngine (Separate out MapEngine)
-   [378](https://github.com/fugue-project/fugue/issues/378) Refactor DataFrame show
-   [377](https://github.com/fugue-project/fugue/issues/377) Create bag
-   [372](https://github.com/fugue-project/fugue/issues/372) Infer execution engine from input
-   [340](https://github.com/fugue-project/fugue/issues/340) Migrate to plugin mode
-   [369](https://github.com/fugue-project/fugue/issues/369) Remove execution from FugueWorkflow context manager, remove engine from FugueWorkflow
-   [373](https://github.com/fugue-project/fugue/issues/373) Fixed Spark engine rename slowness when there are a lot of columns

## 0.7.3

-   [362](https://github.com/fugue-project/fugue/issues/362) Remove Python 3.6 Support
-   [363](https://github.com/fugue-project/fugue/issues/363) Create IbisDataFrame and IbisExecutionEngine
-   [364](https://github.com/fugue-project/fugue/issues/364) Enable Map type support
-   [365](https://github.com/fugue-project/fugue/issues/365) Support column names starting with numbers
-   [361](https://github.com/fugue-project/fugue/issues/361) Better error message for cross join

## 0.7.2

-   [348](https://github.com/fugue-project/fugue/issues/348) Make create data error more informative
-   [349](https://github.com/fugue-project/fugue/issues/349) Ray integration, phase 1: transformation and IO

## 0.7.1

-   [345](https://github.com/fugue-project/fugue/issues/345): Enabled file as input/output for transform and out_transform

## 0.7.0

-   [326](https://github.com/fugue-project/fugue/pull/326): Added tests for Python 3.6 - 3.10 for Linux and 3.7 - 3.9 for Windows. Updated devenv and CICD to Python 3.8.
-   [321](https://github.com/fugue-project/fugue/issues/321): Moved out Fugue SQL to https://github.com/fugue-project/fugue-sql-antlr, removed version cap of `antlr4-python3-runtime`
-   [323](https://github.com/fugue-project/fugue/issues/323): Removed version cap of DuckDB
-   [334](https://github.com/fugue-project/fugue/issues/334): Replaced RLock with SerializableRLock
-   [337](https://github.com/fugue-project/fugue/issues/337): Fixed index warning in fugue_dask
-   [339](https://github.com/fugue-project/fugue/pull/339): Migrated execution engine parsing to triad conditional_dispatcher
-   [341](https://github.com/fugue-project/fugue/issues/341): Added Dask Client to DaskExecutionEngine, and fixed bugs of Dask and Duckdb

## 0.6.6

-   Create a hybrid [engine](https://github.com/fugue-project/fugue/issues/300) of DuckDB and Dask
-   [Save](https://github.com/fugue-project/fugue/issues/285) Spark-like partitioned parquet files for all engines
-   Enable DaskExecutionEngine to transform dataframes with [nested](https://github.com/fugue-project/fugue/issues/299) columns
-   A [smarter](https://github.com/fugue-project/fugue/issues/304) way to determine default npartitions in Dask
-   Support [even partitioning](https://github.com/fugue-project/fugue/issues/303) on Dask
-   Add handling of [nested ArrayType](https://github.com/fugue-project/fugue/issues/308) on Spark
-   Change to [plugin approach](https://github.com/fugue-project/fugue/issues/310) to avoid explicit import
-   Fixed [Click version issue](https://github.com/fugue-project/fugue/issues/313)
-   Added version caps for [antlr4-python3-runtime](https://github.com/fugue-project/fugue/issues/318) and [duckdb](https://github.com/fugue-project/fugue/issues/322) as they both released new versions with breaking changes.

## 0.6.5

-   Make Fugue exceptions [short and useful](https://github.com/fugue-project/fugue/issues/277)
-   Ibis [integration](https://github.com/fugue-project/fugue/issues/272) (experimental)
-   Get rid of [simple assignment](https://github.com/fugue-project/fugue/issues/276) (not used at all)
-   [Improve DuckDB](https://github.com/fugue-project/fugue/pull/289) engine to use a real DuckDB ExecutionEngine
-   YIELD [LOCAL](https://github.com/fugue-project/fugue/issues/284) DATAFRAME

## 0.6.4

-   Add an [option](https://github.com/fugue-project/fugue/issues/267) to transform to turn off native dataframe output
-   Add [callback](https://github.com/fugue-project/fugue/issues/256) parameter to `transform` and `out_transform`
-   Support [DuckDB](https://github.com/fugue-project/fugue/issues/259)
-   Create [fsql_ignore_case](https://github.com/fugue-project/fugue/issues/253) for convenience, make this an option in notebook [setup](https://github.com/fugue-project/fugue/issues/263)
-   Make Fugue SQL error more informative about [case issue](https://github.com/fugue-project/fugue/issues/254)
-   Enable pandas default SQL engine (QPD) to take [lower case SQL](https://github.com/fugue-project/fugue/issues/255)

## 0.6.3

-   Change pickle to cloudpickle for [Flask RPC Server](https://github.com/fugue-project/fugue/issues/246)
-   [Add license](https://github.com/fugue-project/fugue/pull/245) to package

## 0.6.1

-   Parsed [arbitrary object](https://github.com/fugue-project/fugue/issues/234) into execution engine
-   Made Fugue SQL [accept](https://github.com/fugue-project/fugue/issues/233) `+`, `~`, `-` in schema expression
-   Fixed transform [bug](https://github.com/fugue-project/fugue/issues/232) for Fugue DataFrames
-   Fixed a very rare [bug](https://github.com/fugue-project/fugue/issues/239) of annotation parsing

## 0.6.0

-   Added Select, Aggregate, Filter, Assign [interfaces](https://github.com/fugue-project/fugue/issues/211)
-   Made [compatible](https://github.com/fugue-project/fugue/issues/224) with Windows OS, added github actions to test on windows
-   Register [built-in](https://github.com/fugue-project/fugue/issues/191) extensions
-   Accept [platform dependent](https://github.com/fugue-project/fugue/issues/229) annotations for dataframes and execution engines
-   Let SparkExecutionEngine accept [empty](https://github.com/fugue-project/fugue/issues/217) pandas dataframes
-   Move to [codecov](https://github.com/fugue-project/fugue/issues/216)
-   Let Fugue SQL take input dataframes with name such as [a.b](https://github.com/fugue-project/fugue/issues/215)

## 0.5.6

-   Dask repartitioning [improvement](https://github.com/fugue-project/fugue/issues/201)
-   [Separate](https://github.com/fugue-project/fugue/issues/192) Dask IO to use its own APIs
-   Improved Dask print function by adding back [head](https://github.com/fugue-project/fugue/issues/205)
-   Made `assert_or_throw` [lazy](https://github.com/fugue-project/fugue/issues/206)
-   Improved notebook [setup handling](https://github.com/fugue-project/fugue/issues/192) for jupyter lab

## 0.5.5

-   HOTFIX [avro support](https://github.com/fugue-project/fugue/issues/200)

## 0.5.4

-   Added built in [avro support](https://github.com/fugue-project/fugue/issues/125)
-   Fixed dask print [bug](https://github.com/fugue-project/fugue/issues/195)

## 0.5.3

-   Fixed multi take [issue](https://github.com/fugue-project/fugue/issues/184) for dask
-   Fixed pandas, dask print [slow](https://github.com/fugue-project/fugue/issues/186)

## 0.5.2

-   Added Codacy and Slack channel badges, [fixed pylint](https://github.com/fugue-project/fugue/pull/177)
-   Created [transform and out_transform](https://github.com/fugue-project/fugue/issues/181) functions
-   Added partition syntax [sugar](https://github.com/fugue-project/fugue/issues/183)
-   Fixed FugueSQL `CONNECT` [bug](https://github.com/fugue-project/fugue/pull/175)

## 0.5.1

-   Fugueless [1](https://github.com/fugue-project/fugue/issues/108) [2](https://github.com/fugue-project/fugue/issues/149) [3](https://github.com/fugue-project/fugue/issues/164) [4](https://github.com/fugue-project/fugue/issues/153) [5](https://github.com/fugue-project/fugue/issues/152)
-   Notebook experience and extension [1](https://github.com/fugue-project/fugue/issues/159) [2](https://github.com/fugue-project/fugue/issues/160)
-   NativeExecutionEngine: [switched](https://github.com/fugue-project/fugue/issues/171) to use QPD for SQL
-   Spark pandas udf: [migrate](https://github.com/fugue-project/fugue/issues/163) to applyInPandas and mapInPandas
-   SparkExecutionEngine [take bug](https://github.com/fugue-project/fugue/issues/166)
-   Fugue SQL: [PRINT](https://github.com/fugue-project/fugue/issues/154) ROWS n -> PRINT n ROWS|ROW
-   Refactor [yield](https://github.com/fugue-project/fugue/issues/168)
-   Fixed Jinja templating [issue](https://github.com/fugue-project/fugue/issues/134)
-   Change [\_parse_presort_exp](https://github.com/fugue-project/fugue/issues/135) from a private function to public
-   Failure to delete execution temp directory is annoying was changed to [info](https://github.com/fugue-project/fugue/issues/162)

## 0.5.0

-   [Limit and Limit by Partition](https://github.com/fugue-project/fugue/issues/128)
-   [README code](https://github.com/fugue-project/fugue/issues/132) is working now
-   Limit was renamed to [take and added to SQL interface](https://github.com/fugue-project/fugue/issues/136)
-   RPC for [Callbacks](https://github.com/fugue-project/fugue/issues/139) to collect information from workers in real time
-   Changes in handling [input dataframe determinism](https://github.com/fugue-project/fugue/issues/144). This fixes a bug related to [thread locks with Spark DataFrames](https://github.com/fugue-project/fugue/issues/143) because of a deepcopy.

## 0.4.9

-   [sample](https://github.com/fugue-project/fugue/issues/120) function
-   Make csv [infer schema](https://github.com/fugue-project/fugue/issues/121) consistent cross engine
-   Make [loading](https://github.com/fugue-project/fugue/issues/122) file more consistent cross engine

## 0.4.8

-   Support \*\*kwargs in interfaceless extensions, see [this](https://github.com/fugue-project/fugue/issues/107)
-   Support `Iterable[pd.DataFrame]` as output type, see [this](https://github.com/fugue-project/fugue/issues/106)
-   [Alter](https://github.com/fugue-project/fugue/issues/110) column types
-   [RENAME](https://github.com/fugue-project/fugue/issues/114) in Fugue SQL
-   [CONNECT](https://github.com/fugue-project/fugue/issues/112) different SQL service in Fugue SQL
-   Fixed Spark EVEN REPARTITION [issue](https://github.com/fugue-project/fugue/issues/119)

## 0.4.7

-   Add hook to print/show, see [this](https://github.com/fugue-project/fugue/issues/104).

## 0.4.6

-   Fixed import [issue](https://github.com/fugue-project/fugue/issues/99) with OutputTransformer
-   Added [fillna](https://github.com/fugue-project/fugue/issues/95) as a built-in transform, including SQL implementation

## 0.4.5

-   [Extension validation](https://github.com/fugue-project/fugue/issues/81) interface and interfaceless syntax
-   Passing dataframes cross workflow ([yield](https://github.com/fugue-project/fugue/pull/94))
-   [OUT TRANSFORM](https://github.com/fugue-project/fugue/issues/82) to transform and finish a branch of execution
-   Fixed a PandasDataFrame datetime [issue](https://github.com/fugue-project/triad/issues/59) that only happened in transformer interface approach

## 0.4.3

-   Unified checkpoints and persist
-   Drop columns and na implementations in both programming and sql interfaces
-   Presort takes array as input
-   Fixed jinja template rendering issue
-   Fixed path format detection bug

## 0.4.2

-   Require pandas 1.0 because of parquet schema
-   Improved Fugue SQL extension parsing logic
-   Doc for contributors to setup their environment

## 0.4.1

-   Added set operations to programming interface: `union`, `subtract`, `intersect`
-   Added `distinct` to programming interface
-   Ensured partitioning follows SQL convention: groups with null keys are NOT removed
-   Switched `join`, `union`, `subtract`, `intersect`, `distinct` to QPD implementations, so they follow SQL convention
-   Set operations in Fugue SQL can directly operate on Fugue statemens (e.g. `TRANSFORM USING t1 UNION TRANSFORM USING t2`)
-   Fixed bugs
-   Added onboarding document for contributors

## &lt;=0.4.0

-   Main features of Fugue core and Fugue SQL
-   Support backends: Pandas, Spark and Dask
