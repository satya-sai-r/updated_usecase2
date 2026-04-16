# How to Setup and Start the Payment Reminder System

This guide provides step-by-step instructions to set up and start the Payment Reminder Agent System.

## Prerequisites

- Python 3.10 or higher
- Docker Desktop (Windows) or Docker Engine (Linux/Mac)
- Git (optional, for cloning)

---

## Step 1: Start Docker Infrastructure

The system relies on Docker services for messaging, database, and email testing.

```powershell
# Navigate to the docker directory
cd docker

# Start all infrastructure services
docker-compose up -d
```

### Services Started:
| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL | 5432 | Database for transactions |
| NATS | 4222, 8222 | Message broker (monitoring at http://localhost:8222) |
| Duckling | 8000 | Date/time parsing AI |
| Mailpit | 1025, 8025 | SMTP/IMAP testing (UI at http://localhost:8025) |

### Verify Services:
```powershell
# Check all containers are running
docker-compose ps

# Check logs if needed
docker-compose logs -f nats
docker-compose logs -f postgres
```

---

## Step 2: Setup Python Environment

### Create Virtual Environment:
```powershell
# From project root directory
cd ..  # if still in docker folder

# Create virtual environment
python -m venv venv

# Activate virtual environment (Windows PowerShell)
.\venv\Scripts\Activate.ps1

# Activate virtual environment (Windows CMD)
.\venv\Scripts\activate.bat

# Activate virtual environment (Linux/Mac)
source venv/bin/activate
```

### Install Dependencies:
```powershell
# Install all required packages
pip install -r requirements.txt
```

**Key packages:**
- `streamlit` - Dashboard UI
- `nats-py` - Messaging
- `psycopg[binary]` - PostgreSQL async driver
- `pandas`, `duckdb` - Data processing
- `portalocker` - File locking (for Windows)

---

## Step 3: Configure Environment Variables

Create a `.env` file in the project root if not exists:

```env
# Database
POSTGRES_DSN=postgresql://postgres:postgres@localhost:5432/payment_agents

# NATS Messaging
NATS_URL=nats://localhost:4222

# Duckling AI Parser
DUCKLING_URL=http://localhost:8000

# Email (SMTP via Mailpit)
SMTP_HOST=localhost
SMTP_PORT=1025
FROM_EMAIL=noreply@example.com

# IMAP (Mailpit)
IMAP_HOST=localhost
IMAP_PORT=1143
IMAP_USER=admin
IMAP_PASS=admin

# Retailer Email Mapping (JSON format)
RETAILER_EMAIL_MAP={"R001": "retailer1@example.com", "R002": "retailer2@example.com"}
DEFAULT_RECIPIENT=spuvvala@gitam.in
```

---

## Step 4: Initialize Database

The database schema is automatically created by agents, but you can verify connectivity:

```powershell
# Test database connection
python -c "import psycopg; print(psycopg.connect('postgresql://postgres:postgres@localhost:5432/payment_agents'))"
```

---

## Step 5: Start Background Agents

Each agent runs in a separate terminal/process. Start them in order:

### Terminal 1: State Write Agent
```powershell
# Ensure venv is activated
.\venv\Scripts\Activate.ps1

# Start the agent
python agents/state_write_agent.py
```

**Purpose:** Listens for `reminder.sent` and `reply.parsed` events, updates JSON state and PostgreSQL database.

### Terminal 2: Email Dispatch Agent
```powershell
.\venv\Scripts\Activate.ps1
python agents/email_dispatch_agent.py
```

**Purpose:** Sends reminder emails when `reminder.due` messages are received.

### Terminal 3: Reply Monitor Agent
```powershell
.\venv\Scripts\Activate.ps1
python agents/reply_monitor_agent.py
```

**Purpose:** Polls IMAP inbox for retailer replies and publishes `reply.received` events.

### Terminal 4: Reply Parser Agent
```powershell
.\venv\Scripts\Activate.ps1
python agents/reply_parser_agent.py
```

**Purpose:** Parses email replies using Duckling to extract promised payment dates.

### Terminal 5: Ingestion Agent (Optional)
```powershell
.\venv\Scripts\Activate.ps1
python agents/ingestion_agent.py
```

**Purpose:** Watches `uploads/` folder for new Excel files and ingests transactions.

### Terminal 6: Escalation Agent (Optional)
```powershell
.\venv\Scripts\Activate.ps1
python agents/escalation_agent.py
```

**Purpose:** Daily check for overdue transactions requiring escalation.

---

## Step 6: Start Dashboard

```powershell
# In a new terminal
.\venv\Scripts\Activate.ps1

# Start Streamlit dashboard
streamlit run dashboard.py
```

**Access:** http://localhost:8501

**Features:**
- Select Distributor → Retailer → Transaction Date
- View pending transactions
- Select and send reminder emails
- View incoming replies with IST timestamps

---

## Step 7: Test the System

### 1. Check Mailpit for Emails
- Open http://localhost:8025
- This shows all sent/received emails

### 2. Upload Test Data
- Place an Excel file in `uploads/` folder
- File should have columns: `distributor_id`, `retailer_id`, `sku_name`, `product_category_snapshot`, `secondary_transaction_id`, `transaction_date`, `secondary_gross_value`, `secondary_tax_amount`, `secondary_net_value`

### 3. Send Test Email from Dashboard
1. Select Distributor and Retailer
2. Select Transaction Date
3. Check "Select" checkbox for a transaction
4. Click "APPROVE & SEND SELECTED"
5. Check Mailpit at http://localhost:8025 for the email

### 4. Simulate Reply
1. In Mailpit, click "Compose"
2. Send reply to the reminder email with subject containing transaction ID
3. Include text like "Will pay on 20th April"
4. Reply Monitor will detect it, Reply Parser will extract date
5. Check dashboard "Incoming Feed" for the reply

---

## Quick Reference: All Start Commands

```powershell
# 1. Docker (in docker/)
docker-compose up -d

# 2. Activate environment (each terminal)
.\venv\Scripts\Activate.ps1

# 3. Start agents (each in separate terminal)
python agents/state_write_agent.py
python agents/email_dispatch_agent.py
python agents/reply_monitor_agent.py
python agents/reply_parser_agent.py

# 4. Start dashboard
streamlit run dashboard.py
```

---

## Troubleshooting

### Port Already in Use
```powershell
# Find and kill process using port
netstat -ano | findstr :8501
taskkill /PID <PID> /F
```

### Database Connection Failed
- Verify PostgreSQL container: `docker-compose ps`
- Check logs: `docker-compose logs postgres`
- Verify credentials in `.env` file

### NATS Connection Failed
- Verify NATS container is running
- Check NATS monitoring: http://localhost:8222

### File Locking Errors (WinError 32)
- Ensure `portalocker` is installed: `pip install portalocker`
- The state_write_agent now has retry logic with exponential backoff

### Timezone Issues
- All timestamps are converted to IST (UTC+5:30) in dashboard
- Agents use `datetime.now(IST)` for consistent timestamps

### Agent Crashes
- Check individual agent logs in `logs/` folder
- Restart the specific agent

---

## Step 8: Stopping the System

### Stop the Dashboard
- Press `Ctrl+C` in the terminal running Streamlit
- Or close the terminal window

### Stop Background Agents
For each agent terminal:
- Press `Ctrl+C` to gracefully stop
- Or close the terminal windows

### Stop Docker Services
```powershell
# Navigate to docker directory
cd "C:\Users\HOSHITHA MOURIYA\Desktop\usecase2-main\docker"

# Stop all containers
docker-compose down

# To stop and remove volumes (clears database data):
docker-compose down -v
```

### Full System Shutdown Script (PowerShell)
```powershell
# Stop all Python processes (agents and dashboard)
Get-Process python | Stop-Process -Force

# Stop Docker
cd "C:\Users\HOSHITHA MOURIYA\Desktop\usecase2-main\docker"
docker-compose down
```

---

## System Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Dashboard  │────▶│    NATS     │◀────│   Agents    │
│  (Streamlit)│     │  (Messaging)│     │             │
└─────────────┘     └──────┬──────┘     │ • State Write│
                           │            │ • Email      │
                           ▼            │ • Reply Mon  │
                    ┌─────────────┐      │ • Reply Parse│
                    │  PostgreSQL │      └─────────────┘
                    │   (Data)    │
                    └─────────────┘
                           ▲
                           │
                    ┌─────────────┐
                    │  JSON State │
                    │ system_state│
                    └─────────────┘
```

---

## File Structure

```
usecase2-main/
├── agents/                 # Background agent scripts
│   ├── state_write_agent.py
│   ├── email_dispatch_agent.py
│   ├── reply_monitor_agent.py
│   ├── reply_parser_agent.py
│   ├── ingestion_agent.py
│   ├── escalation_agent.py
│   └── sheet_builder_agent.py
├── data/                   # JSON state file
│   └── system_state.json
├── docker/                 # Docker compose
│   ├── docker-compose.yml
│   └── postgres-init.sql
├── logs/                   # Agent logs
├── outputs/                # Generated Excel sheets
├── templates/              # Email templates
├── uploads/                # Incoming Excel files
├── dashboard.py            # Streamlit UI
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables
└── how_to_setup_system.md  # This file
```

---

## Support

For issues or questions, check the logs in `logs/` folder and verify all Docker services are healthy.
