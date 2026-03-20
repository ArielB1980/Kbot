#!/bin/bash
# =============================================================================
# TradingSystem Fresh Droplet Deployment Script
# Run this on the new droplet as root
# =============================================================================

set -e  # Exit on any error

echo "🚀 TradingSystem Fresh Deployment Starting..."
echo "================================================"

# -----------------------------------------------------------------------------
# Phase 1: System Update & Dependencies
# -----------------------------------------------------------------------------
echo ""
echo "📦 Phase 1: Installing system dependencies..."

apt update && apt upgrade -y
apt install -y \
    python3.12 \
    python3.12-venv \
    python3-pip \
    git \
    docker.io \
    docker-compose-v2 \
    ufw \
    fail2ban \
    htop \
    curl

# Enable Docker
systemctl enable docker
systemctl start docker

echo "✅ System dependencies installed"

# -----------------------------------------------------------------------------
# Phase 2: Security Setup
# -----------------------------------------------------------------------------
echo ""
echo "🔒 Phase 2: Configuring firewall..."

ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH
ufw allow 8080/tcp comment 'Dashboard'
ufw --force enable

echo "✅ Firewall configured"

# -----------------------------------------------------------------------------
# Phase 3: User & Directory Setup
# -----------------------------------------------------------------------------
echo ""
echo "👤 Phase 3: Creating trading user..."

# Create trading user if doesn't exist
if ! id "trading" &>/dev/null; then
    useradd -m -s /bin/bash trading
fi
usermod -aG docker trading

# Setup directories
mkdir -p /home/trading
chown trading:trading /home/trading

echo "✅ User configured"

# -----------------------------------------------------------------------------
# Phase 4: Clone Repository
# -----------------------------------------------------------------------------
echo ""
echo "📥 Phase 4: Cloning repository..."

cd /home/trading

# Remove existing if present
rm -rf TradingSystem

# Clone
git clone https://github.com/ArielB1980/TradingSystem.git
cd TradingSystem

# Set ownership
chown -R trading:trading /home/trading/TradingSystem

echo "✅ Repository cloned"

# -----------------------------------------------------------------------------
# Phase 5: Python Environment
# -----------------------------------------------------------------------------
echo ""
echo "🐍 Phase 5: Setting up Python environment..."

sudo -u trading python3.12 -m venv venv
sudo -u trading ./venv/bin/pip install --upgrade pip
sudo -u trading ./venv/bin/pip install -r requirements.txt

echo "✅ Python environment ready"

# -----------------------------------------------------------------------------
# Phase 6: Start Database
# -----------------------------------------------------------------------------
echo ""
echo "🗄️ Phase 6: Starting PostgreSQL..."

docker compose up -d postgres

# Wait for postgres to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 10

# Verify
docker ps | grep postgres

echo "✅ Database running"

# -----------------------------------------------------------------------------
# Phase 7: Create Systemd Services
# -----------------------------------------------------------------------------
echo ""
echo "⚙️ Phase 7: Creating systemd services..."

# Trading Bot Service
cat > /etc/systemd/system/trading-bot.service << 'EOF'
[Unit]
Description=TradingSystem Bot
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
User=trading
WorkingDirectory=/home/trading/TradingSystem
ExecStart=/home/trading/TradingSystem/venv/bin/python -m src.entrypoints.prod_live
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

# Dashboard Service
cat > /etc/systemd/system/trading-dashboard.service << 'EOF'
[Unit]
Description=TradingSystem Dashboard
After=network.target

[Service]
Type=simple
User=trading
WorkingDirectory=/home/trading/TradingSystem
ExecStart=/home/trading/TradingSystem/venv/bin/streamlit run src/dashboard/streamlit_app.py --server.port 8080 --server.address 0.0.0.0 --server.headless true
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable trading-bot trading-dashboard

echo "✅ Systemd services created"

# -----------------------------------------------------------------------------
# Phase 8: Create Environment Template
# -----------------------------------------------------------------------------
echo ""
echo "📝 Phase 8: Creating environment file template..."

cat > /home/trading/TradingSystem/.env << 'EOF'
# =============================================================================
# PRODUCTION CONFIGURATION
# =============================================================================
# IMPORTANT: Fill in your Kraken API keys below, then restart the bot:
#   systemctl restart trading-bot
# =============================================================================

# Environment
ENV=production
ENVIRONMENT=production

# Trading Mode - SET TO 0 FOR LIVE TRADING
DRY_RUN=1

# Database
DATABASE_URL=postgresql://trading_user:trading_pass@localhost:5432/kraken_futures_trading

# Logging
LOG_LEVEL=INFO

# =============================================================================
# KRAKEN API CREDENTIALS (REQUIRED FOR LIVE TRADING)
# =============================================================================

# Spot API (for market data)
KRAKEN_API_KEY=YOUR_SPOT_API_KEY_HERE
KRAKEN_API_SECRET=YOUR_SPOT_API_SECRET_HERE

# Futures API (for trading)
KRAKEN_FUTURES_API_KEY=YOUR_FUTURES_API_KEY_HERE
KRAKEN_FUTURES_API_SECRET=YOUR_FUTURES_API_SECRET_HERE
EOF

chown trading:trading /home/trading/TradingSystem/.env
chmod 600 /home/trading/TradingSystem/.env

echo "✅ Environment template created"

# -----------------------------------------------------------------------------
# Phase 9: Run Database Migrations
# -----------------------------------------------------------------------------
echo ""
echo "🔄 Phase 9: Running database migrations..."

cd /home/trading/TradingSystem
sudo -u trading ./venv/bin/python migrate_schema.py

echo "✅ Database migrations complete"

# -----------------------------------------------------------------------------
# Done!
# -----------------------------------------------------------------------------
echo ""
echo "================================================"
echo "🎉 DEPLOYMENT COMPLETE!"
echo "================================================"
echo ""
echo "NEXT STEPS:"
echo "1. Edit API keys: nano /home/trading/TradingSystem/.env"
echo "2. Set DRY_RUN=0 for live trading"
echo "3. Start bot: systemctl start trading-bot"
echo "4. Start dashboard: systemctl start trading-dashboard"
echo ""
echo "USEFUL COMMANDS:"
echo "  View bot logs:     journalctl -u trading-bot -f"
echo "  View dashboard:    http://$(curl -s ifconfig.me):8080"
echo "  Restart bot:       systemctl restart trading-bot"
echo "  Check status:      systemctl status trading-bot"
echo ""
