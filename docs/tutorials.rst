
Fugue Tutorials
================

To directly read the tutorials without running them:

.. toctree::

   For Beginners <https://fugue-tutorials.readthedocs.io/en/latest/tutorials/beginner.html>
   For Advanced Users <https://fugue-tutorials.readthedocs.io/en/latest/tutorials/advanced.html>


You may launch a
`Fugue tutorial notebook environemnt on binder <https://mybinder.org/v2/gh/fugue-project/tutorials/master>`_

**But it runs slow on binder**, the machine on binder isn't powerful enough for
a distributed framework such as Spark. Parallel executions can become sequential, so some of the
performance comparison examples will not give you the correct numbers.

Alternatively, you should get decent performance if running its docker image on your own machine:

.. code-block:: bash

    docker run -p 8888:8888 fugueproject/tutorials:latest





