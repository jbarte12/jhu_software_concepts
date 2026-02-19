# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Module 4'
copyright = '2026, Jayna Bartel'
author = 'Jayna Bartel'
release = '1'

import os
import sys
sys.path.insert(0, os.path.abspath("../.."))       # repo root — finds src/
sys.path.insert(0, os.path.abspath("../../tests")) # finds test modules

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  # needed for :param:/:type:/:returns: style docstrings
]

napoleon_use_param = True
napoleon_use_rtype = True

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']