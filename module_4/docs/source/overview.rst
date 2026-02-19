Overview & Setup
=================

What this project does
-----------------------

This application scrapes graduate school application results from GradCafe,
runs each record through a local LLM to standardize program and university
names, stores the results in PostgreSQL, and serves live statistics through
a Flask dashboard.

The pipeline has three stages:

1. **Pull Data** — scrapes new applicant records from GradCafe and saves
   them to a local JSON file.
2. **Update Analysis** — reads the JSON file, calls the LLM to normalize
   each record, and syncs the results into the database.
3. **Render** — queries the database and renders the stats page.

Prerequisites
-------------

- Python 3.11+
- PostgreSQL 15+ running locally
- The TinyLlama GGUF model file placed in ``models/``

Environment setup
-----------------

**1. Clone the repo and create a virtual environment:**

.. code-block:: bash

   git clone <your-repo-url>
   cd module_4
   python -m venv .venv
   source .venv/bin/activate      # Windows: .venv\Scripts\activate
   pip install -r requirements.txt

**2. Set up the database:**

The app connects to PostgreSQL using the credentials hardcoded in
``src/load_data.py``:

.. code-block:: text

   host:     127.0.0.1
   port:     5432
   database: sm_app
   user:     postgres
   password: abc123

Create the database before running:

.. code-block:: bash

   psql -U postgres -c "CREATE DATABASE sm_app;"

**3. Place the LLM model:**

Download ``tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf`` and place it at:

.. code-block:: text

   module_4/models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf

Running the application
-----------------------

.. code-block:: bash

   cd module_4
   python src/run.py

The app will be available at ``http://127.0.0.1:5000``.

Running the tests
-----------------

Run all tests:

.. code-block:: bash

   pytest

Run a specific marker group:

.. code-block:: bash

   pytest -m integration -v
   pytest -m db -v
   pytest -m analysis -v
   pytest -m buttons -v
   pytest -m web -v

See :doc:`testing` for the full testing guide.
