To run the system on your machine, follow these steps in order. Open a PowerShell window and navigate to C:\Users\HOSHITHA MOURIYA\Desktop\usecase2_geminiCLI-main.

  1. Start the Infrastructure (Docker)
  The system requires NATS (messaging) and Duckling (AI parsing) to be running in Docker.

   1 # Navigate to the docker folder and start services
   2 cd C:\Users\DELL\projects\payment-agent
   3 docker compose -f docker\docker-compose.yml up -d
  Verify: Open http://localhost:8222 (http://localhost:8222) to see if NATS is running.

  ---

  2. Start the Background Agents
  You need to run the agents that handle the actual work (sending emails and watching for replies). You can run these in one go:

   1 # Activate the virtual environment
   2 .\venv\Scripts\Activate.ps1
   3
   4 # Start all 4 agents in the background
   Start-Process -FilePath ".\venv\Scripts\python.exe" -ArgumentList "agents\email_dispatch_agent.py" -WindowStyle Hidden
   Start-Process -FilePath ".\venv\Scripts\python.exe" -ArgumentList "agents\reply_monitor_agent.py" -WindowStyle Hidden
   Start-Process -FilePath ".\venv\Scripts\python.exe" -ArgumentList "agents\reply_parser_agent.py" -WindowStyle Hidden
   Start-Process -FilePath ".\venv\Scripts\python.exe" -ArgumentList "agents\state_write_agent.py" -WindowStyle Hidden

  ---

  3. Start the UI Dashboard
  This is your "Command Center" where you select and approve transactions.
   1 # Run the dashboard
   2 .\venv\Scripts\streamlit run dashboard.py
  The dashboard will automatically open in your browser at http://localhost:8501.
