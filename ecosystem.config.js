const fs   = require('fs');
const path = require('path');

// Parse .env file — no dotenv dependency needed
const env = {};
try {
  fs.readFileSync(path.join(__dirname, '.env'), 'utf8')
    .split('\n')
    .forEach(line => {
      const [k, ...v] = line.trim().split('=');
      if (k && !k.startsWith('#')) env[k] = v.join('=');
    });
} catch (_) {}

const APP_NAME = env.APP_NAME || 'livetradingbot';
const PORT     = env.PORT     || '8765';

module.exports = {
  apps: [
    {
      name: APP_NAME,
      script: "main.py",
      args: "",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "512M",
      restart_delay: 3000,
      interpreter: __dirname + "/venv/bin/python",
      cwd: __dirname + "/bot",
      env: {
        NODE_ENV: "production",
        PORT: PORT,
      },
      error_file: "../logs/err.log",
      out_file: "../logs/out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
  ],
};
