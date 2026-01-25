# Import create_app - in app/__init__.py
from app import create_app

# Create instance of the Flask app, named "app"
app = create_app()

# Allow for direct execution (python run.py)
if __name__ == "__main__":

    # Run on host '0.0.0.0' and port 8080
    app.run(host = '0.0.0.0', port = 8080, debug =True)