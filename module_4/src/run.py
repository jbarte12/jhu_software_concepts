# Import the Flask application factory function
from src.app import create_app

# Only run this block when the file is executed directly
if __name__ == "__main__":

    # Create a new Flask application instance
    app = create_app()

    # Start the Flask development server with debugging enabled
    # use_reloader=False prevents the app from starting twice
    app.run()
