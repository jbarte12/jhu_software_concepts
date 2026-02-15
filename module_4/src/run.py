# src/run.py
import sys
from pathlib import Path

# Add src folder to Python path
sys.path.append(str(Path(__file__).parent))  # Adds ./src to sys.path

from app import create_app  # Now Python can find the app package

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
