# Updating education data

## Create a virtual environment

I typically use python's venv package. You may need to install this.

From the base folder of the project, run

```shell
python -m venv env
```

`env` in this command is your environment name.

### Activate the environment

For Windows:

In the folder with the env file run

```shell
env\scripts\Activate.ps1
```

For OSX or Linux

```shell
source env/bin/activate
```

## Install requirements

Once your environment is activated, from the project root folder, install requirements with

```shell
pip install -r requrements.txt
```

## Installing this package -> IMPORTANT

You need to install the package itself as 'editable.'

```bash
> pip install -e .
```

## When running a file you have to run from the root file

```bash
> python ./eem_schools/2024/eem_schools_2024_etl.py
```

## Order of operations

### Late summer
- *are we able to get preview access?*
- *does the freep?* -- their reports are published Aug 31st

- Update EEM
- Update assessments
    - 3g ELA
    - 8g Math
    - College readiness
- *Others ?*

### Late winter
