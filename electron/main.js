const { app, BrowserWindow, dialog } = require('electron');
const { spawn } = require('child_process');
const path = require('path');
const http = require('http');

let serverProcess = null;
let mainWindow = null;

const HOST = '127.0.0.1';
const PORT = 8000;
const BASE_URL = `http://${HOST}:${PORT}`;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    show: false,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true
    }
  });

  mainWindow.loadURL(BASE_URL);

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

function waitForServer(url, retries = 20, interval = 500) {
  return new Promise((resolve, reject) => {
    const attempt = (remaining) => {
      http.get(url, (res) => {
        res.resume();
        resolve();
      }).on('error', () => {
        if (remaining <= 0) {
          reject(new Error('FastAPI server did not start in time.'));
          return;
        }
        setTimeout(() => attempt(remaining - 1), interval);
      });
    };

    attempt(retries);
  });
}

function startFastAPIServer() {
  // Development example only:
  // Better for production: use a bundled python/backend executable.
  serverProcess = spawn(
    'python',
    ['-m', 'uvicorn', 'backend.main:app', '--host', HOST, '--port', String(PORT)],
    {
      shell: true,
      stdio: 'inherit',
      cwd: app.getAppPath()
    }
  );

  serverProcess.on('close', (code) => {
    console.log(`FastAPI server exited with code ${code}`);
  });

  serverProcess.on('error', (err) => {
    console.error('Failed to start FastAPI server:', err);
  });
}

function stopFastAPIServer() {
  if (serverProcess && !serverProcess.killed) {
    serverProcess.kill();
    serverProcess = null;
  }
}

app.whenReady().then(async () => {
  try {
    startFastAPIServer();
    await waitForServer(BASE_URL);
    createWindow();

    app.on('activate', () => {
      if (BrowserWindow.getAllWindows().length === 0) {
        createWindow();
      }
    });
  } catch (err) {
    console.error(err);
    dialog.showErrorBox(
      'Startup Error',
      'The backend server failed to start.'
    );
    app.quit();
  }
});

app.on('before-quit', () => {
  stopFastAPIServer();
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});