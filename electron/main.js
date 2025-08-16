const { app, BrowserWindow } = require('electron');
const path = require('path');
const { spawn } = require('child_process');
const http = require('http');
const os = require('os');

let serverProcess = null;
let mainWindow = null;

// Get local IP address dynamically
function getLocalIPAddress() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return '127.0.0.1';
}

// Wait until the server is ready before creating the Electron window
function waitForServer(url, callback, retries = 20, interval = 500) {
  const check = () => {
    http.get(url, () => {
      console.log("✅ FastAPI server is up!");
      callback();
    }).on('error', err => {
      if (retries === 0) {
        console.error("❌ FastAPI server did not start in time.");
      } else {
        setTimeout(() => waitForServer(url, callback, retries - 1, interval), interval);
      }
    });
  };
  check();
}

function createWindow(baseUrl) {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true
    }
  });

  mainWindow.loadURL(`${baseUrl}/`);

  mainWindow.on('closed', function () {
    mainWindow = null;
  });
}

function startFastAPIServer(ip) {
  serverProcess = spawn('uvicorn', ['backend.main:app', '--host', '0.0.0.0', '--port', '8000'], {
    shell: true,
    stdio: 'inherit',
  });

  serverProcess.on('close', (code) => {
    console.log(`FastAPI server exited with code ${code}`);
  });

  serverProcess.on('error', (err) => {
    console.error('Failed to start FastAPI server:', err);
  });
}

app.whenReady().then(() => {
  const localIP = getLocalIPAddress();
  const baseUrl = `http://${localIP}:8000`;

  startFastAPIServer(localIP);

  waitForServer(baseUrl, () => {
    createWindow(baseUrl);
  });

  app.on('activate', function () {
    if (BrowserWindow.getAllWindows().length === 0) createWindow(baseUrl);
  });
});

// Gracefully kill FastAPI when Electron exits
app.on('window-all-closed', function () {
  if (process.platform !== 'darwin') {
    if (serverProcess) serverProcess.kill();
    app.quit();
  }
});
