# Spark Learning Project

Install spark according to instructions on website, add the following to `~/.bashrc` and `~/.zshrc`:

    # Set python home as local var to wherever your python is at
    PYTHON_HOME=$HOME/.local/share/rtx/installs/python/3.10

    # Set the spark home env var to wherever you installed spark
    export SPARK_HOME=$HOME/Tools/spark-3.3.4-bin-hadoop3

    # Set or update the following env vars
    export PYSPARK_PYTHON=$PYTHON_HOME/bin/python
    export PYSPARK_DRIVER_PYTHON=$PYTHON_HOME/bin/python
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$SPARK_HOME/python/lib/pyspark.zip:$PYTHONPATH
    export PATH=$PYTHON_HOME/bin:$SPARK_HOME/bin:$PATH

