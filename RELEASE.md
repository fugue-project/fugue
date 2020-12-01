# Release Notes

## 0.4.8

* Support **kwargs in interfaceless extensions, see [this](https://github.com/fugue-project/fugue/issues/107)
* Support Iterable[pd.DataFrame] as output type, see [this](https://github.com/fugue-project/fugue/issues/106)
* [Alter](https://github.com/fugue-project/fugue/issues/110) column types
* [RENAME](https://github.com/fugue-project/fugue/issues/114) in Fugue SQL
* [CONNECT](https://github.com/fugue-project/fugue/issues/112) different SQL service in Fugue SQL

## 0.4.7

* Add hook to print/show, see [this](https://github.com/fugue-project/fugue/issues/104).

## 0.4.6

* Fixed import [issue](https://github.com/fugue-project/fugue/issues/99) with OutputTransformer
* Added [fillna](https://github.com/fugue-project/fugue/issues/95) as a built-in transform, including SQL implementation

## 0.4.5

* [Extension validation](https://github.com/fugue-project/fugue/issues/81) interface and interfaceless syntax
* Passing dataframes cross workflow ([yield](https://github.com/fugue-project/fugue/pull/94))
* [OUT TRANSFORM](https://github.com/fugue-project/fugue/issues/82) to transform and finish a branch of execution
* Fixed a PandasDataFrame datetime [issue](https://github.com/fugue-project/triad/issues/59) that only happened in transformer interface approach

## 0.4.3

* Unified checkpoints and persist
* Drop columns and na implementations in both programming and sql interfaces
* Presort takes array as input
* Fixed jinja template rendering issue
* Fixed path format detection bug

## 0.4.2

* Require pandas 1.0 because of parquet schema
* Improved Fugue SQL extension parsing logic
* Doc for contributors to setup their environment

## 0.4.1

* Added set operations to programming interface: `union`, `subtract`, `intersect`
* Added `distinct` to programming interface
* Ensured partitioning follows SQL convention: groups with null keys are NOT removed
* Switched `join`, `union`, `subtract`, `intersect`, `distinct` to QPD implementations, so they follow SQL convention
* Set operations in Fugue SQL can directly operate on Fugue statemens (e.g. `TRANSFORM USING t1 UNION TRANSFORM USING t2`)
* Fixed bugs
* Added onboarding document for contributors

## <=0.4.0

* Main features of Fugue core and Fugue SQL
* Support backends: Pandas, Spark and Dask
