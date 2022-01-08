Community
==========

The Fugue project is not only about Fugue framework. The goal is to build the abstraction layer on
different computing and machine learning frameworks so that developers and scientists can focus more
on their own tasks and focus more on WHAT to do instead of HOW to do and WHERE to do. We have a very
diversed open source world, many frameworks are great only on certain things but they try to create
their own ecosystems excluding others. We believe that we deserve a more unified approach to distributed
computing and machine learning, and we deserve the freedom of choosing different options.


Ask a question
--------------

Please join `Slack chat <https://fugue-project.slack.com/join/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ#>`_
to ask questions. We will try to reply as soon as possible.


Request bug fix or new features
-------------------------------

You can request bug fix or new features at `<https://github.com/fugue-project/fugue/issues>`_, before submitting
a new request, it will be great if you can firstly contact us through
`slack <https://fugue-project.slack.com/join/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ#>`_.


Contribute
----------

Fugue is a very new project, we truly appreciate if you can contribute code, ideas or docs. Please reach out
using `slack <https://fugue-project.slack.com/join/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ#>`_, we will
be excited to chat with you.

Creating a development environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are three steps to setting-up a development environment

#. Create a virtual environment with your choice of environment manager
#. Install the requirements
#. Install the git hook scripts

**Creating an environment**

Below are examples for how to create and activate an environment in virtualenv and conda.

Using virtualenv

.. code-block:: bash

   python3 -m venv venv
   . venv/bin/activate

Using conda

.. code-block:: bash

   conda create --name fugue-dev
   conda activate fugue-dev

**Installing requirements**

The Fugue repo has a Makefile that can be used to install the requirements. It supports installation in both
pip and conda. Instructions to install `make` for Windows users can be found later.

Pip install requirements

.. code-block:: bash

   make setupinpip

Conda install requirements

.. code-block:: bash

   make setupinconda

Manually install requirements

For Windows users who don't have the `make` command, you can use your package manager of choice. For pip:

.. code-block:: bash

   pip3 install -r requirements.txt

For Anaconda users, first install pip in the newly created environment. If pip install is used without installing pip, conda will use
the system-wide pip

.. code-block:: bash

   conda install pip
   pip install -r requirements.txt

**Notes for Windows Users**

For Windows users, you will need to download Microsoft C++ Build Tools found [here](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

`make` is a GNU command that does not come with Windows. An installer can be downloaded [here](http://gnuwin32.sourceforge.net/packages/make.htm)
After installing, add the bin to your PATH environment variable.

**Installing git hook scripts**

Fugue has pre-commit hooks to check if code is appropriate to be commited. The previous `make` command installs this.
If you installed the requirements manually, install the git hook scripts with:

.. code-block:: bash

   pre-commit install
