# Project Template

This template provides a starter kit for structuring our Data Science/Machine Learning projects. Use this template if you need to do any exploratory data analysis, machine learning, or run code in the KMi Big Data Cluster.

Follow the examples provided and adjust the structure of this repository to what works for you and for your project.

The code in this repository uses **Python 3+** (was tested with Python 3.6.4).

## Contents of this readme

* [Project skeleton](#markdown-header-project-skeleton)
    * [Contents of this readme](#markdown-header-contents-of-this-readme)
    * [Setup](#markdown-header-setup)
        * [Mandatory](#markdown-header-mandatory)
        * [Optional](#markdown-header-optional)
    * [Running the app](#markdown-header-running-the-app)
    * [Repository organization](#markdown-header-repository-organization)
    * [Using Spark](#markdown-header-using-spark)
    * [Using Machine Learning](#markdown-header-using-machine-learning)
        * [Suggestions on how to structure ML experiments for reproducibility](#markdown-header-suggestions-on-how-to-structure-ml-experiments-for-reproducibility)
            * [Data](#markdown-header-data)
            * [Modelling](#markdown-header-modelling)
    * [Using Jupyter Notebooks](#markdown-header-using-jupyter-notebooks)
    * [Using multiple config files](#markdown-header-using-multiple-config-files)
    * [Other suggestions](#markdown-header-other-suggestions)
    * [Resources](#markdown-header-resources)
        * [Other resources](#markdown-header-other-resources)


## Setup

Do these steps after you have cloned this repository.

### Mandatory

* Copy `config.json.example` to `config.json` and edit as needed
* Copy `logging.json.example` to `logging.json` and edit as needed
* Install dependencies using ``pip install --upgrade pip && pip install -r requirements.txt``
* Download NLTK data (if planning to use NLTK) with ``python -m nltk.downloader all``
* Download SpaCy data (in planning to use SpaCy) with ``python -m spacy download en``

### Optional

* Create and start a Python virtual environment
    * There are a number of different libraries for creating virtual environments in Python:
        * The standard built-in `venv` library works in Python 3 only
        * Most people seem to recommend the third-party `virtualenv` library due to ease of use and support for both Python 2 and 3
    * Additionally, there are libraries which extend the functionality or `venv` and `virtualenv`:
        * `virtualenvwrapper` is an extension of `virtualenv` which is useful in case you need to manage multiple virtual environments
        * `pipenv` combines `virtualenv` and `pip` into one command, also automates creation of a requirements file, plus other features
    * This template will work with any of the above libraries
    * A good tutorial on how to use `virtualenv` with `virtualenvwrapper` is available [here](http://www.marinamele.com/2014/07/install-python3-on-mac-os-x-and-use-virtualenv-and-virtualenvwrapper.html).
    * A tutorial on how to start using `pipenv` is provided [here](https://dev.to/yukinagae/your-first-guide-to-getting-started-with-pipenv-50bn).
* If you use a virtual environment, add a new kernel to your jupyter installation:
    * While in your virtual environment run ``python -m ipykernel install --user --name name_of_your_environment``
    * Installed kernels can be listed using ``jupyter kernelspec list``

## Running the app

* Run `python run.py` to print app menu or `python run.py -h` to print help for command line arguments.
* To supply a specific config file to the app: `python run.py -c custom_config.json`.
* To supply a list of actions that will be directly executed: `python run.py -a action_1 action_2`, e.g. `python run.py -a 0 1`.
* The command line arguments can be used to cycle through different configurations for different experiments and execute all of them in order.

## Repository organization

* **``data/``**
    * **``external/``:** data from third party sources
    * **``interim/``:** intermediate data that has been transformed (i.e. not raw and not final)
    * **``processed/``:** the final, canonical data sets for modeling
    * **``raw/``:** the original, immutable data dump (e.g. dumps of CORE DB tables)
* **``documents/``:** various documents related to the project (presentations, notes, etc.)
* **``models/``:** serialized models, embeddings, configs, etc.
* **``notebooks/``:** jupyter notebooks
* **``output/``:** generated analysis (reports, results, figures, etc.)
* **``spark/``:** PySpark code to be run on the KMI cluster, instructions provided in section [Using Spark](#markdown-header-using-spark)
* **``src/``:** source code for use in this project
* **``config.json.example``:** project configuration such as paths, API keys, etc.
* **``logging.json.example``:** logging configuration
* **``README.md``:** this file
* **``requirements.txt``:** project dependencies
* **``run.py``:** entry point for scripts in ``src/`` directory

## Using Spark

The Spark template in the ``spark/`` directory is based on:

* [Best Practices Writing Production-Grade PySpark Jobs](https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f#.xh5bpelr2)

Detailed instructions on how to use the KMI Big Data cluster are provided in [CORE Wiki](https://bitbucket.org/kmi-ou/core/wiki/KMI%20cluster).

The ``spark/`` directory is structured in the following way:

* **``jobs/``:** each subdirectory in this directory represents one PySpark job
* **``shared/``:** code shared between different jobs
* **``config.json.example``:** example config file for Spark jobs
* **``main.py``:** entry point for all Spark code, takes care of executing jobs
* **`Makefile`:** used for packaging Spark scripts before uploading them to the cluster
* **``spark_submit.sh``:** script for executing spark-submit jobs

To execute a spark job in the ``spark/jobs/`` directory:

* ``cd ./spark``
* ``make all``
    * This creates a ``./spark/dist/`` directory
* ``scp dist/* cluster:~``
* SSH to the cluster and run ``./spark_submit job_name``
    * You might need to run ``chmod +x spark_submit.sh`` first
* Go to the [Yarn manager](https://manager.bigdata.kmi.open.ac.uk/yarn-rm/cluster) and monitor progress of your job

This template contains an example ``word_count`` job, executing it with ``./spark_submit word_count`` should produce a results file in [Hue](https://hue.bigdata.kmi.open.ac.uk/hue/filebrowser/view=/project/core/test_job_output/) and the following output (that can be viewed in the Yarn manager):

```
...
2019-07-17 17:39:28 shared.log_utils INFO: DataFrame: word_count
2019-07-17 17:39:28 shared.log_utils INFO: Data schema:
root
 |-- word: string (nullable = true)
 |-- count: long (nullable = true)

2019-07-17 17:39:30 shared.log_utils INFO: Number of rows: 74
2019-07-17 17:39:30 shared.log_utils INFO: First 5 records
+----+-----+
|word|count|
+----+-----+
| and|    6|
|  to|    6|
|from|    5|
|CORE|    4|
|data|    4|
+----+-----+
only showing top 5 rows

2019-07-17 17:39:31 jobs.word_count INFO: Storing results in /project/core/test_job_output
...
```

General suggestions:

* Use logging, do not `print` (file `main.py` takes care of setting up logging in a way that lets you view logging output in the Yarn manager)
* Store paths in `config.json`
* Describe the order in which your jobs should be run in a `README` or a script
    * You can edit `spark_submit.sh` to execute all of your jobs in the correct order
    * If one job is dependent on the output of the previous job, you can change `--conf spark.yarn.submit.waitAppCompletion` to true to force `spark-submit` to wait for each job to complete

## Using Machine Learning

In general, an ML pipeline follows the following steps:

* Data loading and splitting
* Feature extraction
* Training, which involves:
    * Data transformations (e.g. outlier removal, missing value detection)
    * Model training, scoring, and tuning
* Evaluation
    * Metrics
    * Plots

The ``src/`` directory models this workflow. Organization of the directory:

* **``data/``:** classes/scripts for data loading/preprocessing
* **``evaluation/``:** model evaluation
* **``features/``:** classes/scripts for feature extraction
* **``models/``:** transformations and models
* **``tasks/``:** scripts that put everything together
* **``utils/``:** various utilities, such as file and logging utilities
* **``visualization/``:** any code that produces plots

### Suggestions on how to structure ML experiments for reproducibility

#### Data

* All steps taken to collect, clean, and preprocess data should be written in code and committed to git
    * If some steps cannot be written in code, consider creating a README listing steps taken to obtain data (e.g. SQL queries, URLs, etc.)
* **Storage:**
    * [Raw immutable data](https://drivendata.github.io/cookiecutter-data-science/#data-is-immutable) (raw DB exports, raw labels, etc.) should be stored in ``data/raw``
        * Do not rewrite old files, version/date them instead
    * Final processed data which will be fed into analysis scripts/ML models (e.g. specific splits) should be stored in ``data/processed``
    * Where possible, upload final processed versions of data to git
    * If the final dataset is too large to be stored in git, write down the location of where it is stored
* **Naming conventions:**
    * Keep historic versions of data in ``data/raw`` and ``data/processed`` where appropriate, date or otherwise label each version
* **Example:**
    * Run ``python run.py`` in project root and select option `0`
    * This should produce the following:
        * Unprocessed dataset in `data/raw`
        * A new directory in `data/processed` named with current date, the directory should contain:
            * Full processed dataset
            * Subdirectory containing training, development, and testing splits

#### Modelling

* Where possible, the following information should be written down in a config file(s) or at least a readme:
    * Version of data/splits used
    * Which features/feature extraction methods/data transformations were used
    * All model parameters
* After each experiment, save the config file with the above information along with the results
* For full reproducibility, create a git tag for each experiment with `git tag -a v1.5 -m "Description of the experiment"`, afterwards save the tag name along with the results
* **Example:**
    * Run `python run.py` and select option `1`
    * This should produce a directory in `output/experiments/`, named with current date, time, and model name
    * The directory should contain:
        * A copy of app and model configs, experiment results, a list of installed libraries and a file with system information
    * Commit all code changes and experiment results
    * Create a new git tag with `git tag -a tagname -m "Tag description"`
    * Attach the name of the previously created tag to the name of the newly created directory, e.g. `experiments/201909061410_dummy_tagname`
    * To run a different model, open `config.json` and change the following parameters:
        * Set `model:config` to `models/configs/lr.json`
        * Set `model:model_class` to `LR`
        * Re-run `python run.py` while selection option `1`
* **Resources:**
    * [GitHub repo](https://github.com/benhamner/Metrics) with implementations of many evaluation metrics in Python

## Using Jupyter Notebooks

* `notebooks/00_template_start_here.ipynb` notebook contains basic setup
* Follow suggestions provided [here](https://drivendata.github.io/cookiecutter-data-science/#notebooks-are-for-exploration-and-communication)

## Using multiple config files

* The template enables using multiple application configuration files
* By default, it expects the main application config to be store in `config.json` in the root directory
* To supply a different config file, simply pass the name of the config file as a `-c` command line argument when running `run.py` script, e.g.: `python run.py -c config_custom.json`

## Other suggestions

* Follow these code conventions: [PEP 008 -- Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/)

## Resources

This template is based on:

* [Cookiecutter Data Science](https://drivendata.github.io/cookiecutter-data-science/)
* [Best Practices Writing Production-Grade PySpark Jobs](https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f#.xh5bpelr2)

### Other resources

* Excellent [presentation](https://docs.google.com/presentation/d/1n2RlMdmv1p25Xy5thJUhkKGvjtV-dkAIsUXP-AL4ffI/edit#slide=id.g362da58057_0_1) by Joel Grus of Allen AI discussing why Jupyter notebooks encourage bad coding habits
* [The Practice of Reproducible Research](https://www.practicereproducibleresearch.org/) -- free book
* [Ten Simple Rules for Reproducible Computational Research](https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1003285)
* [Good Enough Practices in Scientific Computing](https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1005510)
* [How We Conduct Deep Learning Research at niland.io](https://www.linkedin.com/pulse/how-we-conduct-research-nilandio-christophe-charbuillet/?articleId=6209006084158427136)
* [How to Plan and Run Machine Learning Experiments Systematically](https://machinelearningmastery.com/plan-run-machine-learning-experiments-systematically/)
* [How to set up a perfect Python project](https://sourcery.ai/blog/python-best-practices/)
* Structure and automated workflow for a machine learning project: 
    * [Part 1](https://towardsdatascience.com/structure-and-automated-workflow-for-a-machine-learning-project-2fa30d661c1e)
    * [Part 2](https://towardsdatascience.com/structure-and-automated-workflow-for-a-machine-learning-project-part-2-b5b420625102)
