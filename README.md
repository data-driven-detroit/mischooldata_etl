# Updating education data

## Creating a `config.toml`

In order to access the database strings automatically, you need to create a `config.toml` file in the base directory.

It should have the following structure, pointing at EDW.

```toml
[app]
name="education"

[db]
user=""
password=""
host=""
name="data"
port=5432

metadata_schema="metadata"
```

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

## IMPORTANT: Installing this package 

You need to install the package itself as 'editable.'

```bash
> pip install -e .
```

## When running a file you have to run from the root file

```bash
> python ./eem_schools/2024/eem_schools_2024_etl.py
```


## Recoding Standards

### Two-digit grade strings

All grades should be represented as two digit strings. Sometimes these values are included in category breakouts (hence the string) where they are reported as two-digit strings. Even if the grade category does have other report groups on it, the 2-digit string standard should be followed.

## Release Dates

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
