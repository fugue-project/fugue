# Release Notes

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
