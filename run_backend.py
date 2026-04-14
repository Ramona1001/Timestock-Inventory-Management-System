import os
import sys
import traceback
import uvicorn
from backend.main import app

# Resolve paths for both dev and PyInstaller, mainly use for desktop packaging with PyInstaller. 
# This ensures that when we access files (like the database or templates), 
# we can find them correctly whether we're running from source or from a packaged executable.
def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

if __name__ == "__main__":
    try:
        print("Starting FastAPI backend...")

        # Example debug info (very useful)
        print("Running in packaged mode:", hasattr(sys, '_MEIPASS'))
        print("Base path:", getattr(sys, '_MEIPASS', os.getcwd()))

        uvicorn.run(
            app,
            host="127.0.0.1",
            port=8000,
            log_level="info"
        )

    except Exception as e:
        print("Backend failed to start:")
        print(str(e))
        traceback.print_exc()
        input("Press Enter to exit...")