#!/bin/bash
#
# Comprehensive Deployment Script
# 
# This script:
# 1. Validates local changes
# 2. Runs pre-deployment tests (optional)
# 3. Commits and pushes to GitHub (using GITHUB_TOKEN)
# 4. SSH to production server and pulls latest code
# 5. Restarts the trading system service
# 6. Verifies deployment
#
# Usage:
#   ./scripts/deploy.sh [--skip-tests] [--skip-commit] [--message "commit message"]
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Load environment variables from .env.local if it exists
if [ -f .env.local ]; then
    set -a
    source .env.local
    set +a
fi

# Configuration (can be overridden by .env.local)
SERVER="${DEPLOY_SERVER:-root@207.154.193.121}"
SSH_KEY="${DEPLOY_SSH_KEY:-$HOME/.ssh/trading_droplet}"
TRADING_USER="${DEPLOY_TRADING_USER:-trading}"
TRADING_DIR="${DEPLOY_TRADING_DIR:-/home/trading/TradingSystem}"
SERVICE_NAME="${DEPLOY_SERVICE_NAME:-trading-bot.service}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"

# Parse command line arguments
SKIP_TESTS=false
SKIP_COMMIT=false
SKIP_REPLAY="${SKIP_REPLAY:-false}"
COMMIT_MESSAGE=""
FORCE_PUSH=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --skip-commit)
            SKIP_COMMIT=true
            shift
            ;;
        --skip-replay)
            SKIP_REPLAY=true
            shift
            ;;
        --message)
            COMMIT_MESSAGE="$2"
            shift 2
            ;;
        --force)
            FORCE_PUSH=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Usage: $0 [--skip-tests] [--skip-commit] [--skip-replay] [--message \"commit message\"] [--force]"
            exit 1
            ;;
    esac
done

# Function to print colored output
print_step() {
    echo -e "\n${BLUE}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
print_step "Checking prerequisites..."

# Check if SSH key exists
if [ ! -f "$SSH_KEY" ]; then
    print_error "SSH key not found: $SSH_KEY"
    exit 1
fi

# Check if git is available
if ! command -v git &> /dev/null; then
    print_error "git is not installed"
    exit 1
fi

# Check if we're in a git repository
if [ ! -d .git ]; then
    print_error "Not in a git repository"
    exit 1
fi

# Check for uncommitted changes (unless skipping commit)
if [ "$SKIP_COMMIT" = false ]; then
    if [ -n "$(git status --porcelain)" ]; then
        print_warning "Uncommitted changes detected"
        git status --short
        echo ""
        read -p "Continue with commit? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_error "Deployment cancelled"
            exit 1
        fi
    fi
fi

print_success "Prerequisites check passed"

# Step 1: Run pre-deployment tests (unless skipped)
if [ "$SKIP_TESTS" = false ]; then
    print_step "Running pre-deployment tests..."
    if command -v make &> /dev/null; then
        if make smoke 2>&1 | tee /tmp/deploy-smoke.log; then
            print_success "Smoke tests passed"
        else
            print_error "Smoke tests failed"
            print_warning "Use --skip-tests to bypass"
            exit 1
        fi
    else
        print_warning "make not found, skipping tests"
    fi
else
    print_warning "Skipping pre-deployment tests"
fi

# Step 1b: Run replay gate (unless skipped)
if [ "$SKIP_REPLAY" = "true" ] || [ "$SKIP_REPLAY" = "1" ]; then
    print_warning "Skipping replay gate (SKIP_REPLAY=$SKIP_REPLAY)"
else
    print_step "Running replay gate (seed=42)..."
    if command -v make &> /dev/null; then
        if make replay SEED=42 2>&1 | tee /tmp/deploy-replay.log; then
            print_success "Replay gate passed (6/6 episodes)"
        else
            print_error "Replay gate FAILED — episodes did not pass"
            print_warning "Use --skip-replay or SKIP_REPLAY=1 to bypass"
            exit 1
        fi
    else
        print_warning "make not found, skipping replay gate"
    fi
fi

# Step 2: Commit and push to GitHub (unless skipped)
if [ "$SKIP_COMMIT" = false ]; then
    print_step "Preparing to commit and push to GitHub..."
    
    # Check if we have a GitHub token
    if [ -z "$GITHUB_TOKEN" ]; then
        print_warning "GITHUB_TOKEN not set in .env.local"
        print_warning "Will attempt to push using existing git credentials"
    fi
    
    # Get current branch
    CURRENT_BRANCH=$(git branch --show-current)
    print_step "Current branch: $CURRENT_BRANCH"
    
    # Generate commit message if not provided
    if [ -z "$COMMIT_MESSAGE" ]; then
        COMMIT_MESSAGE="Deploy: $(date +'%Y-%m-%d %H:%M:%S')"
    fi
    
    # Stage all changes
    print_step "Staging changes..."
    git add -A
    
    # Check if there are changes to commit
    if [ -z "$(git diff --cached --name-only)" ]; then
        print_warning "No changes to commit"
    else
        # Commit changes
        print_step "Committing changes..."
        git commit -m "$COMMIT_MESSAGE" || {
            print_error "Commit failed (maybe no changes?)"
            exit 1
        }
        print_success "Changes committed"
    fi
    
    # Push to GitHub
    print_step "Pushing to GitHub..."
    
    # Configure git to use token if provided
    if [ -n "$GITHUB_TOKEN" ]; then
        # Extract repo URL
        REPO_URL=$(git remote get-url origin)
        # Convert to HTTPS with token
        if [[ $REPO_URL == git@* ]]; then
            # Convert SSH URL to HTTPS
            REPO_URL=$(echo $REPO_URL | sed 's/git@github.com:/https:\/\/github.com\//' | sed 's/\.git$//')
        fi
        # Add token to URL
        REPO_URL_WITH_TOKEN=$(echo $REPO_URL | sed "s|https://|https://${GITHUB_TOKEN}@|")
        git remote set-url origin "$REPO_URL_WITH_TOKEN" || true
    fi
    
    # Push (with force if requested)
    if [ "$FORCE_PUSH" = true ]; then
        print_warning "Force pushing to $CURRENT_BRANCH"
        git push --force origin "$CURRENT_BRANCH" || {
            print_error "Failed to push to GitHub"
            exit 1
        }
    else
        git push origin "$CURRENT_BRANCH" || {
            print_error "Failed to push to GitHub"
            print_warning "If you need to force push, use --force flag"
            exit 1
        }
    fi
    
    print_success "Pushed to GitHub: $CURRENT_BRANCH"
    
    # Restore original remote URL if we modified it
    if [ -n "$GITHUB_TOKEN" ]; then
        ORIGINAL_URL=$(git remote get-url origin | sed "s|https://${GITHUB_TOKEN}@|https://|")
        git remote set-url origin "$ORIGINAL_URL" 2>/dev/null || true
    fi
else
    print_warning "Skipping commit and push (using --skip-commit)"
fi

# Step 3: Deploy to production server
print_step "Deploying to production server: $SERVER"

# Test SSH connection
print_step "Testing SSH connection..."
if ssh -i "$SSH_KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SERVER" "echo 'SSH connection successful'" 2>/dev/null; then
    print_success "SSH connection successful"
else
    print_error "Failed to connect to server via SSH"
    print_warning "Check:"
    print_warning "  1. SSH key permissions: chmod 600 $SSH_KEY"
    print_warning "  2. Server is accessible: ping $(echo $SERVER | cut -d@ -f2)"
    print_warning "  3. SSH key is authorized on server"
    exit 1
fi

# Pull latest code on server
print_step "Pulling latest code on server..."
ssh -i "$SSH_KEY" "$SERVER" << DEPLOY_EOF
    set -e
    echo "📂 Changing to $TRADING_DIR"
    cd $TRADING_DIR || {
        echo "❌ Directory not found: $TRADING_DIR"
        exit 1
    }
    
    echo "📋 Current branch:"
    su - $TRADING_USER -c "cd $TRADING_DIR && git branch --show-current"
    
    echo "⬇️  Fetching latest changes..."
    su - $TRADING_USER -c "cd $TRADING_DIR && git fetch origin"
    
    echo "🔄 Resetting to origin/main..."
    su - $TRADING_USER -c "cd $TRADING_DIR && git reset --hard origin/main"
    
    echo "📦 Installing/updating dependencies..."
    # Use python -m pip: some venvs ship non-executable pip stubs (Permission denied on venv/bin/pip).
    # Prefer requirements.txt when present; otherwise editable install from pyproject (uv-style repos).
    if [ -d "$TRADING_DIR/venv" ]; then
        su - $TRADING_USER -c "cd $TRADING_DIR && venv/bin/python -m pip install --upgrade pip"
        if su - $TRADING_USER -c "test -f $TRADING_DIR/requirements.txt"; then
            su - $TRADING_USER -c "cd $TRADING_DIR && venv/bin/python -m pip install -r requirements.txt"
        else
            su - $TRADING_USER -c "cd $TRADING_DIR && venv/bin/python -m pip install -e ."
        fi
    elif [ -d "$TRADING_DIR/.venv" ]; then
        su - $TRADING_USER -c "cd $TRADING_DIR && .venv/bin/python -m pip install --upgrade pip"
        if su - $TRADING_USER -c "test -f $TRADING_DIR/requirements.txt"; then
            su - $TRADING_USER -c "cd $TRADING_DIR && .venv/bin/python -m pip install -r requirements.txt"
        else
            su - $TRADING_USER -c "cd $TRADING_DIR && .venv/bin/python -m pip install -e ."
        fi
    else
        echo "⚠️  No venv found; assuming dependencies installed via system Python or other method"
    fi
    
    echo "📝 Recent commits:"
    su - $TRADING_USER -c "cd $TRADING_DIR && git log --oneline -5"
    
    echo "✅ Code updated successfully"
DEPLOY_EOF

if [ $? -eq 0 ]; then
    print_success "Code updated on server"
else
    print_error "Failed to update code on server"
    exit 1
fi

# Resolve service name on remote host (fallback for legacy installs)
if ! ssh -i "$SSH_KEY" "$SERVER" "systemctl cat $SERVICE_NAME >/dev/null 2>&1"; then
    if [ -z "${DEPLOY_SERVICE_NAME:-}" ] && ssh -i "$SSH_KEY" "$SERVER" "systemctl cat trading-system.service >/dev/null 2>&1"; then
        print_warning "Service '$SERVICE_NAME' not found; falling back to legacy 'trading-system.service'"
        SERVICE_NAME="trading-system.service"
    else
        print_error "Service not found on server: $SERVICE_NAME"
        print_warning "Set DEPLOY_SERVICE_NAME in .env.local to the correct unit (e.g., trading-bot.service)"
        exit 1
    fi
fi

# Step 3b: Ensure cron jobs are installed (idempotent)
print_step "Ensuring cron jobs are installed for user '$TRADING_USER'..."
CRON_LINE="0 3 * * 0 ${TRADING_DIR}/scripts/weekly_replay_sweep.sh >> ${TRADING_DIR}/logs/cron-replay.log 2>&1"
ssh -i "$SSH_KEY" "$SERVER" << CRON_EOF
    # Read existing crontab (suppress "no crontab" warning)
    EXISTING=\$(su - $TRADING_USER -c "crontab -l 2>/dev/null" || true)

    if echo "\$EXISTING" | grep -qF "weekly_replay_sweep.sh"; then
        echo "Cron job already installed — skipping"
    else
        # Append to existing crontab (preserve any other entries)
        (echo "\$EXISTING"; echo ""; echo "# Weekly replay sweep — seeds 1-5, Sundays 3AM UTC"; echo "$CRON_LINE") | su - $TRADING_USER -c "crontab -"
        echo "Cron job installed for $TRADING_USER"
    fi
CRON_EOF

if [ $? -eq 0 ]; then
    print_success "Cron jobs verified"
else
    print_warning "Could not verify cron jobs (non-fatal)"
fi

# Step 4: Restart service
print_step "Restarting service: $SERVICE_NAME"
ssh -i "$SSH_KEY" "$SERVER" "systemctl restart $SERVICE_NAME" || {
    print_error "Failed to restart service"
    exit 1
}

# Wait a moment for service to start
sleep 3

# Step 5: Verify deployment
print_step "Verifying deployment..."

# Check service status
print_step "Service status:"
ssh -i "$SSH_KEY" "$SERVER" "systemctl status $SERVICE_NAME --no-pager | head -n 20" || true

# Check if service is active
if ssh -i "$SSH_KEY" "$SERVER" "systemctl is-active --quiet $SERVICE_NAME"; then
    print_success "Service is active"
else
    print_error "Service is not active"
    print_warning "Check logs: ssh -i $SSH_KEY $SERVER 'journalctl -u $SERVICE_NAME -n 50 --no-pager'"
    exit 1
fi

# Show recent logs
print_step "Recent logs (last 10 lines):"
ssh -i "$SSH_KEY" "$SERVER" "sudo -u $TRADING_USER tail -n 10 $TRADING_DIR/logs/run.log 2>/dev/null || journalctl -u $SERVICE_NAME -n 10 --no-pager" || true

# Step 6: Run server-only tests (DB + exchange API dependent)
print_step "Running server-side test verification..."
if ssh -i "$SSH_KEY" "$SERVER" "cd $TRADING_DIR && \
    sudo -u $TRADING_USER bash -c 'set -a; source .env; set +a; \
    venv/bin/python -m pytest tests/ -m server -v --tb=short -q' 2>&1"; then
    print_success "Server-side tests passed"
else
    print_warning "Server-side tests failed (non-blocking). Review results above."
fi

# Final summary
echo ""
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ DEPLOYMENT COMPLETE${NC}"
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo ""
echo "📊 Monitor logs with:"
echo "   ssh -i $SSH_KEY $SERVER 'sudo -u $TRADING_USER tail -f $TRADING_DIR/logs/run.log'"
echo ""
echo "📋 Check service status:"
echo "   ssh -i $SSH_KEY $SERVER 'systemctl status $SERVICE_NAME'"
echo ""
