module.exports = {
  apps: [
    {
      name: "livetradingbot",
      script: "main.py",
      cwd: "./bot",
      interpreter: "venv/bin/python",
      interpreter_args: "-u",
      args: "",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "512M",
      restart_delay: 3000,
      env: {
        PYTHONUNBUFFERED: "1",
      },
      error_file: "../logs/err.log",
      out_file: "../logs/out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
  ],
};
