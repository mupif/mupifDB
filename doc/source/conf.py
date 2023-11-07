# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'MuPIF services' 
copyright = '2023, Bořek Patzák, Václav Šmilauer'
author = 'Bořek Patzák, Václav Šmilauer'
# (Czech Technical University, Faculty of Civil Engineering, Department of Mechanics, Thákurova 7, 16629, Prague, Czech Republic.)'

# The full version, including alpha/beta/rc tags
release = '0.x'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    # 'sphinx.ext.autodoc',
    # 'sphinxcontrib.apidoc'
    'sphinxcontrib.openapi',
    'myst_nb',
    'sphinx_rtd_theme',
]

import sys, os.path

thisDir=os.path.dirname(os.path.abspath(__file__))

#apidoc_module_dir=thisDir+'/../../mupif'
#apidoc_output_dir='api/'
#apidoc_toc_file='api'
#apidoc_excluded_paths=[]
#apidoc_module_first=True

source_suffix={
    '.rst':'restructuredtext',
    '.ipynb':'myst-nb',
}
# don't run notebooks at readthedocs, without REST API server
# just put it inline as it is
nb_execution_mode='off'


sys.path.append(thisDir+'/../..')
import mupif
import mupif.tests
import importlib



# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

numfig=True

latex_engine = 'lualatex'
latex_logo='img/mupif-logo.png'
latex_documents=[('index','mupifdb.tex','MuPIF DB Documentation',
    r'Bořek Patzák, Václav Šmilauer \\& others\\ \hbox{} \\ \parbox{.5\linewidth}{\normalsize Czech Technical University \\ Faculty of Civil Engineering \\ Department of Mechanics \\ Thákurova 7 \\ 16629 Prague \\ Czech Republic}','manual')]


# -- Options for HTML output -------------------------------------------------

html_theme='sphinx_rtd_theme'

#html_theme_options=dict(
#    github_banner=True,
#    github_user='mupif',
#    github_repo='mupif',
#    display_github=True
#)
html_context=dict(
    github_banner=True,
    github_user='mupif',
    github_repo='mupifDB',
    display_github=True,
    github_version='dev',
    conf_py_path='doc/source'
)

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# for readthedocs.org, don't try to connect to the DB when importing mupifDB.api.main
import os
os.environ['MUPIFDB_DRY_RUN']='1'

# Generate OpenAPI json description prior to running sphinx
#
# https://github.com/tiangolo/fastapi/issues/1173
import fastapi.openapi.utils
import mupifDB.api.main
import json
with open('mupifdb-rest-api.openapi.json', 'w') as f:
    app=mupifDB.api.main.app
    json.dump(fastapi.openapi.utils.get_openapi(
        title='MupifDB REST API',
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
        # openapi_prefix=app.openapi_prefix,
    ),f)

# copy files from outside of the doc subdirectory here so that they can be included in the docs
import shutil
shutil.copyfile('../../mupifDB/api/edm/jupyter/04-dms-data.ipynb','04-dms-data.ipynb')
