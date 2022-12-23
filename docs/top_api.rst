Top Level User API Reference
============================

.. |SchemaLikeObject| replace:: :ref:`Schema like object <tutorial:tutorials/advanced/x-like:schema>`
.. |ParamsLikeObject| replace:: :ref:`Parameters like object <tutorial:tutorials/advanced/x-like:parameters>`
.. |DataFrameLikeObject| replace:: :ref:`DataFrame like object <tutorial:tutorials/advanced/x-like:dataframe>`
.. |DataFramesLikeObject| replace:: :ref:`DataFrames like object <tutorial:tutorials/advanced/x-like:dataframes>`
.. |PartitionLikeObject| replace:: :ref:`Partition like object <tutorial:tutorials/advanced/x-like:partition>`
.. |RPCHandlerLikeObject| replace:: :ref:`RPChandler like object <tutorial:tutorials/advanced/x-like:rpc>`

.. |ExecutionEngine| replace:: :class:`~fugue.execution.execution_engine.ExecutionEngine`
.. |NativeExecutionEngine| replace:: :class:`~fugue.execution.native_execution_engine.NativeExecutionEngine`
.. |FugueWorkflow| replace:: :class:`~fugue.workflow.workflow.FugueWorkflow`

.. |ReadJoin| replace:: Read Join tutorials on :ref:`workflow <tutorial:tutorials/advanced/dag:join>` and :ref:`engine <tutorial:tutorials/advanced/execution_engine:join>` for details
.. |FugueConfig| replace:: :doc:`the Fugue Configuration Tutorial <tutorial:tutorials/advanced/useful_config>`
.. |PartitionTutorial| replace:: :doc:`the Partition Tutorial <tutorial:tutorials/advanced/partition>`
.. |FugueSQLTutorial| replace:: :doc:`the Fugue SQL Tutorial <tutorial:tutorials/fugue_sql/index>`
.. |DataFrameTutorial| replace:: :ref:`the DataFrame Tutorial <tutorial:tutorials/advanced/schema_dataframes:dataframe>`
.. |ExecutionEngineTutorial| replace:: :doc:`the ExecutionEngine Tutorial <tutorial:tutorials/advanced/execution_engine>`
.. |ZipComap| replace:: :ref:`Zip & Comap <tutorial:tutorials/advanced/execution_engine:zip & comap>`
.. |LoadSave| replace:: :ref:`Load & Save <tutorial:tutorials/advanced/execution_engine:load & save>`
.. |AutoPersist| replace:: :ref:`Auto Persist <tutorial:tutorials/advanced/useful_config:auto persist>`
.. |TransformerTutorial| replace:: :doc:`the Transformer Tutorial <tutorial:tutorials/extensions/transformer>`
.. |CoTransformer| replace:: :ref:`CoTransformer <tutorial:tutorials/advanced/dag:cotransformer>`
.. |CoTransformerTutorial| replace:: :doc:`the CoTransformer Tutorial <tutorial:tutorials/extensions/cotransformer>`
.. |FugueDataTypes| replace:: :doc:`Fugue Data Types <tutorial:tutorials/appendix/generate_types>`

IO
~~

.. autofunction:: fugue.api.as_fugue_dataset

.. autofunction:: fugue.api.as_fugue_df
.. autofunction:: fugue.api.load
.. autofunction:: fugue.api.save



Information
~~~~~~~~~~~

.. autofunction:: fugue.api.count
.. autofunction:: fugue.api.is_bounded
.. autofunction:: fugue.api.is_empty
.. autofunction:: fugue.api.is_local
.. autofunction:: fugue.api.show

.. autofunction:: fugue.api.get_column_names
.. autofunction:: fugue.api.get_schema
.. autofunction:: fugue.api.is_df
.. autofunction:: fugue.api.peek_array
.. autofunction:: fugue.api.peek_dict



Transformation
~~~~~~~~~~~~~~

.. autofunction:: fugue.api.alter_columns
.. autofunction:: fugue.api.drop_columns
.. autofunction:: fugue.api.head
.. autofunction:: fugue.api.normalize_column_names
.. autofunction:: fugue.api.rename
.. autofunction:: fugue.api.select_columns

.. autofunction:: fugue.api.distinct
.. autofunction:: fugue.api.dropna
.. autofunction:: fugue.api.fillna
.. autofunction:: fugue.api.sample
.. autofunction:: fugue.api.take

.. autofunction:: fugue.api.join
.. autofunction:: fugue.api.union
.. autofunction:: fugue.api.intersect
.. autofunction:: fugue.api.subtract

.. autofunction:: fugue.api.transform
.. autofunction:: fugue.api.out_transform

SQL
~~~

.. autofunction:: fugue.api.raw_sql

Conversion
~~~~~~~~~~

.. autofunction:: fugue.api.as_array
.. autofunction:: fugue.api.as_array_iterable
.. autofunction:: fugue.api.as_arrow
.. autofunction:: fugue.api.as_dict_iterable
.. autofunction:: fugue.api.as_pandas
.. autofunction:: fugue.api.get_native_as_df

ExecutionEngine
~~~~~~~~~~~~~~~
.. autofunction:: fugue.api.engine_context
.. autofunction:: fugue.api.set_global_engine
.. autofunction:: fugue.api.clear_global_engine
.. autofunction:: fugue.api.get_current_engine


Big Data Operations
~~~~~~~~~~~~~~~~~~~
.. autofunction:: fugue.api.broadcast
.. autofunction:: fugue.api.persist
.. autofunction:: fugue.api.repartition


Development
~~~~~~~~~~~

.. autofunction:: fugue.api.run_engine_function





