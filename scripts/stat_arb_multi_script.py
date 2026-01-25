"""
Hummingbot Strategy V2 for Multi-Pair Statistical Arbitrage.

This strategy uses StrategyV2Base to properly manage V2 controllers:
- Loads pairs from the selected_pairs database table
- Creates a StatArbV2 controller for each pair
- Controllers receive market_data_provider and actions_queue automatically

Usage:
    1. Deploy using the deploy script:
       ./scripts/deploy_to_hummingbot.sh ~/hummingbot

    2. Configure OKX perpetual connector in Hummingbot:
       connect okx_perpetual_demo

    3. Start the script:
       start --script stat_arb_multi_script.py
"""

from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Set

# Add scripts directory to path for our symlinked modules
import sys
SCRIPTS_DIR = Path(__file__).parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# Hummingbot V2 framework imports
from pydantic import Field
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    StopExecutorAction,
)

# Import OUR modules (symlinked as stat_arb_hbot in Hummingbot scripts/)
from stat_arb_hbot.controllers.stat_arb_v2 import StatArbV2, StatArbV2Config
from stat_arb_hbot.data.pair_loader import PairLoader, SelectedPair

# Optional: database connection and logging - setup logger first for debugging
import logging as _logging
_db_logger = _logging.getLogger(__name__)

DB_AVAILABLE = False
TimescaleDBConnection = None
LiveLogger = None
TradingMode = None
try:
    from trader_src.stat_arb.data.timescaledb import TimescaleDBConnection as TSConn
    TimescaleDBConnection = TSConn
    # LiveLogger is in the symlinked stat_arb_hbot module (same as controller imports)
    from stat_arb_hbot.data.live_logger import LiveLogger as LL, TradingMode as TM
    LiveLogger = LL
    TradingMode = TM
    DB_AVAILABLE = True
    _db_logger.info("Successfully imported TimescaleDBConnection from trader_src and LiveLogger")
except Exception as e:
    _db_logger.warning(f"trader_src import failed: {type(e).__name__}: {e}")
    try:
        from src.stat_arb.data.timescaledb import TimescaleDBConnection as TSConn
        TimescaleDBConnection = TSConn
        from stat_arb_hbot.data.live_logger import LiveLogger as LL, TradingMode as TM
        LiveLogger = LL
        TradingMode = TM
        DB_AVAILABLE = True
        _db_logger.info("Successfully imported TimescaleDBConnection from src and LiveLogger")
    except Exception as e2:
        _db_logger.warning(f"src import also failed: {type(e2).__name__}: {e2}")


logger = logging.getLogger(__name__)


# ============================================================================
# Load config from YAML
# ============================================================================
import yaml

def _load_multi_config() -> dict:
    """
    Load configuration from stat_arb_multi.yaml.

    Searches for config in multiple locations to handle both:
    - Running from trader project directory
    - Running from Hummingbot (where script is symlinked)
    """
    # Resolve symlink to get actual file location in trader project
    script_path = Path(__file__).resolve()  # Resolves symlinks

    config_paths = [
        # From resolved stat_arb_hbot/scripts/ -> trader/config/
        script_path.parent.parent.parent / "config" / "stat_arb_multi.yaml",
        # From trader project root
        Path("/Users/alexeyolkov/code/PycharmProjects/trader/config/stat_arb_multi.yaml"),
        # Current directory fallback
        Path("config/stat_arb_multi.yaml"),
    ]

    for config_path in config_paths:
        if config_path.exists():
            try:
                with open(config_path) as f:
                    config = yaml.safe_load(f)
                    logger.info(f"Loaded config from {config_path}")
                    return config or {}
            except Exception as e:
                logger.warning(f"Failed to load config from {config_path}: {e}")

    logger.warning(f"No config file found, searched: {[str(p) for p in config_paths]}")
    return {}

# Load config at import time
_MULTI_CONFIG = _load_multi_config()

# ============================================================================
# Load pairs from database at module import time (for markets class attribute)
# ============================================================================

# Symbols not available on OKX demo trading (loaded from config)
# Fallback to empty list if not configured
_DEMO_EXCLUDED_SYMBOLS: List[str] = _MULTI_CONFIG.get("demo_excluded_symbols", [])

# Strategy settings from config (with defaults)
_strategy_config = _MULTI_CONFIG.get("strategy", {})
_CONNECTOR_NAME = _strategy_config.get("connector_name", "okx_perpetual_demo")
_MAX_PAIRS = _strategy_config.get("max_pairs", 8)
_IS_DEMO_MODE = "demo" in _CONNECTOR_NAME

if _DEMO_EXCLUDED_SYMBOLS:
    logger.info(f"Loaded {len(_DEMO_EXCLUDED_SYMBOLS)} excluded symbols for demo mode")


def _load_markets_from_db_with_filter(
    connector_name: str,
    max_pairs: int = 8,
    excluded_symbols: Optional[List[str]] = None
) -> Dict[str, Set[str]]:
    """Load markets with optional symbol filtering for demo mode."""
    empty_markets = {connector_name: set()}

    if not DB_AVAILABLE or TimescaleDBConnection is None:
        logger.error("Database not available - cannot load pairs. Script will not trade.")
        return empty_markets

    try:
        db = TimescaleDBConnection()
        loader = PairLoader(db)
        pairs = loader.load_active_pairs(max_pairs=max_pairs, excluded_symbols=excluded_symbols)
        db.close()

        if not pairs:
            logger.error("No pairs found in selected_pairs table. Script will not trade.")
            return empty_markets

        # Extract unique symbols from pairs
        symbols: Set[str] = set()
        for pair in pairs:
            okx_a, okx_b = pair.to_okx_symbols()
            symbols.add(okx_a)
            symbols.add(okx_b)

        logger.info(f"Loaded {len(symbols)} symbols from {len(pairs)} pairs in database")
        return {connector_name: symbols}

    except Exception as e:
        logger.error(f"Failed to load pairs from database: {e}. Script will not trade.")
        return empty_markets


_PRELOADED_MARKETS = _load_markets_from_db_with_filter(
    _CONNECTOR_NAME,
    _MAX_PAIRS,
    excluded_symbols=_DEMO_EXCLUDED_SYMBOLS if _IS_DEMO_MODE else None
)


# ============================================================================
# Strategy Configuration
# ============================================================================

# Pre-extract config sections for class defaults
_capital_config = _MULTI_CONFIG.get("capital", {})
_risk_config = _MULTI_CONFIG.get("risk", {})
_refresh_config = _MULTI_CONFIG.get("refresh", {})


class StatArbMultiConfig(StrategyV2ConfigBase):
    """
    Configuration for Multi-Pair Statistical Arbitrage Strategy.

    Extends StrategyV2ConfigBase to work with the V2 framework.
    Values are loaded from config/stat_arb_multi.yaml with fallbacks to defaults.
    """
    script_file_name: str = os.path.basename(__file__)
    # Override required fields from parent with proper Field defaults
    markets: Dict[str, Set[str]] = Field(default_factory=lambda: _PRELOADED_MARKETS.copy())
    candles_config: List[CandlesConfig] = Field(default_factory=list)
    controllers_config: List[str] = Field(default_factory=list)  # Empty - we load dynamically from DB

    # Strategy-level settings (from YAML strategy section)
    connector_name: str = _CONNECTOR_NAME
    max_pairs: int = _MAX_PAIRS
    trading_mode: str = _strategy_config.get("trading_mode", "demo")

    # Capital settings (from YAML capital section)
    total_capital_quote: Decimal = Decimal(str(_capital_config.get("total_capital_quote", 10000)))
    leverage: int = _capital_config.get("leverage", 5)
    max_capital_per_pair_pct: Decimal = Decimal(str(_capital_config.get("max_capital_per_pair_pct", 0.2)))
    reserve_capital_pct: Decimal = Decimal(str(_capital_config.get("reserve_capital_pct", 0.1)))
    use_db_position_size: bool = _capital_config.get("use_db_position_size", True)

    # Risk settings (from YAML risk section)
    default_take_profit_pct: Decimal = Decimal(str(_risk_config.get("default_take_profit_pct", 0.02)))
    default_stop_loss_pct: Decimal = Decimal(str(_risk_config.get("default_stop_loss_pct", 0.05)))
    default_time_limit_hours: int = _risk_config.get("default_time_limit_hours", 72)

    # Daily refresh settings (from YAML refresh section)
    enable_daily_refresh: bool = _refresh_config.get("enable_daily_refresh", True)
    refresh_hour_utc: int = _refresh_config.get("refresh_hour_utc", 0)
    refresh_minute_utc: int = _refresh_config.get("refresh_minute_utc", 5)

    # Profitability gate (hardcoded for now, could add to YAML)
    use_profitability_gate: bool = True
    trading_cost_pct: Decimal = Decimal("0.0014")
    min_profit_margin: Decimal = Decimal("0.001")

    @property
    def available_capital(self) -> Decimal:
        """Capital available for trading after reserve."""
        return self.total_capital_quote * (1 - self.reserve_capital_pct)

    @property
    def max_position_per_pair(self) -> Decimal:
        """Maximum position size per pair."""
        return self.total_capital_quote * self.max_capital_per_pair_pct


# ============================================================================
# Strategy Implementation
# ============================================================================

class StatArbMultiStrategy(StrategyV2Base):
    """
    Multi-Pair Statistical Arbitrage Strategy using V2 Framework.

    This strategy:
    1. Loads pairs from selected_pairs table in TimescaleDB
    2. Creates a StatArbV2 controller for each pair
    3. Controllers handle signal calculation and order generation
    4. Strategy executes the actions via the V2 executor framework
    """

    # Class attribute required by ScriptStrategyBase
    markets: Dict[str, Set[str]] = _PRELOADED_MARKETS

    @classmethod
    def init_markets(cls, config: StatArbMultiConfig):
        """Initialize markets from pre-loaded database pairs."""
        cls.markets = _PRELOADED_MARKETS

    def __init__(self, connectors: Dict[str, ConnectorBase], config: Optional[StatArbMultiConfig] = None):
        """Initialize the strategy."""
        try:
            # Create default config if not provided
            if config is None:
                config = StatArbMultiConfig()
            # Note: super().__init__ calls initialize_controllers() which sets up
            # _db, _pair_loader, _last_refresh_date, _exit_only_pairs
            logger.info(f"Calling super().__init__ with connectors type={type(connectors)}, keys={list(connectors.keys()) if hasattr(connectors, 'keys') else 'N/A'}")
            logger.info(f"Connectors content: {connectors}")
            super().__init__(connectors, config)
            self.config: StatArbMultiConfig = config
            logger.info("Strategy __init__ completed successfully")
        except Exception as e:
            logger.error(f"Error in StatArbMultiStrategy.__init__: {e}\n{traceback.format_exc()}")
            raise

    def initialize_controllers(self):
        """
        Override to load controllers from database instead of YAML files.

        This is called by StrategyV2Base.__init__() after market_data_provider
        and actions_queue are initialized.
        """
        # Initialize attributes (parent calls this before our __init__ completes)
        self._db = None
        self._pair_loader = None
        self._last_refresh_date = None
        self._exit_only_pairs = {}
        self._shared_live_logger = None  # Shared logger for all controllers

        # Initialize database connection
        if DB_AVAILABLE and TimescaleDBConnection is not None:
            try:
                self._db = TimescaleDBConnection()
                self._pair_loader = PairLoader(self._db)
                logger.info("Database connection initialized")
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                return

        if not self._pair_loader:
            logger.error("No pair loader available - cannot create controllers")
            return

        # Load active pairs from database (filter unavailable symbols for demo)
        excluded = _DEMO_EXCLUDED_SYMBOLS if _IS_DEMO_MODE else None
        pairs = self._pair_loader.load_active_pairs(
            max_pairs=self.config.max_pairs,
            excluded_symbols=excluded
        )

        if not pairs:
            logger.error("No pairs found in database")
            return

        logger.info(f"Loaded {len(pairs)} pairs from database")

        # Create shared LiveLogger for all controllers (ONE session for all pairs)
        if DB_AVAILABLE and LiveLogger is not None and TradingMode is not None:
            try:
                db_for_logger = TimescaleDBConnection()
                trading_mode = TradingMode(self.config.trading_mode)
                self._shared_live_logger = LiveLogger(
                    db=db_for_logger,
                    trading_mode=trading_mode,
                    exchange="okx",
                )
                # Start ONE session for all pairs
                config_snapshot = {
                    "strategy": "stat_arb_multi",
                    "max_pairs": self.config.max_pairs,
                    "connector_name": self.config.connector_name,
                    "trading_mode": self.config.trading_mode,
                    "total_capital_quote": str(self.config.total_capital_quote),
                    "leverage": self.config.leverage,
                }
                self._shared_live_logger.start_session(
                    config=config_snapshot,
                    pairs_loaded=len(pairs),
                )
                logger.info(f"Started shared logging session: {self._shared_live_logger.session_id} with {len(pairs)} pairs")
            except Exception as e:
                logger.warning(f"Failed to create shared LiveLogger: {e}", exc_info=True)
                self._shared_live_logger = None

        # Create controller for each pair (with shared logger)
        for pair in pairs:
            self._create_controller_for_pair(pair)

        self._last_refresh_date = datetime.now(timezone.utc).date()
        logger.info(f"Initialized {len(self.controllers)} controllers")

    @staticmethod
    def _pair_id_to_controller_id(pair_id: str) -> str:
        """Convert pair_id to controller_id format."""
        return f"stat_arb_{pair_id.replace(':', '_')}"

    @staticmethod
    def _controller_id_to_pair_id(controller_id: str) -> str:
        """Convert controller_id back to pair_id format."""
        # Remove 'stat_arb_' prefix and replace '_' back to ':'
        if controller_id.startswith("stat_arb_"):
            return controller_id[9:].replace("_", ":", 1)  # Only replace first underscore
        return controller_id

    def _create_controller_for_pair(self, pair: SelectedPair) -> None:
        """Create and register a controller for a trading pair."""
        try:
            okx_a, okx_b = pair.to_okx_symbols()

            # Calculate position size
            if self.config.use_db_position_size:
                position_size = self.config.total_capital_quote * pair.position_size_pct
            else:
                position_size = self.config.available_capital / self.config.max_pairs

            # Cap at max per pair
            position_size = min(position_size, self.config.max_position_per_pair)

            # Create controller config
            controller_id = self._pair_id_to_controller_id(pair.pair_id)
            controller_config = StatArbV2Config(
                id=controller_id,  # Explicitly set ID (Pydantic v2 validator doesn't trigger on default)
                controller_name=controller_id,
                connector_name=self.config.connector_name,
                trading_pair_a=okx_a,
                trading_pair_b=okx_b,
                hedge_ratio=pair.hedge_ratio,
                zscore_entry_threshold=pair.entry_threshold,
                zscore_exit_threshold=pair.exit_threshold,
                position_size_quote=position_size,
                leverage=self.config.leverage,
                # Triple barrier
                global_take_profit=self.config.default_take_profit_pct,
                global_stop_loss=self.config.default_stop_loss_pct,
                time_limit_seconds=self.config.default_time_limit_hours * 3600,
                # Profitability gate
                use_profitability_gate=self.config.use_profitability_gate,
                trading_cost_pct=self.config.trading_cost_pct,
                min_profit_margin=self.config.min_profit_margin,
                # Trading mode for logging
                trading_mode=self.config.trading_mode,
                # Disable controller's own DB logging - we use shared logger
                enable_db_logging=False,
            )

            # Create controller directly with shared logger (instead of add_controller)
            # This allows us to pass the shared live_logger
            logger.info(f"Creating controller with id={controller_config.id}, shared_logger={self._shared_live_logger is not None}")
            controller = StatArbV2(
                controller_config,
                self.market_data_provider,
                self.actions_queue,
                live_logger=self._shared_live_logger,  # Pass shared logger
            )
            self.controllers[controller_config.id] = controller
            logger.info(f"Controllers dict now has {len(self.controllers)} entries: {list(self.controllers.keys())}")

            logger.info(
                f"Created controller: {pair.pair_id} "
                f"({okx_a}/{okx_b}), HR={pair.hedge_ratio}, "
                f"size=${position_size:.2f}"
            )

        except Exception as e:
            logger.error(f"Failed to create controller for {pair.pair_id}: {e}", exc_info=True)

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        Return empty list - controllers handle their own actions via actions_queue.

        The StrategyV2Base framework calls determine_executor_actions() on controllers
        which put actions into actions_queue, handled by listen_to_executor_actions().
        """
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Return empty list - controllers handle their own stop actions.
        """
        return []

    def start(self, clock, timestamp: float):
        """Override to add logging."""
        try:
            logger.info(f"Strategy start() called with clock={clock}, timestamp={timestamp}")
            super().start(clock, timestamp)
            logger.info("Strategy start() completed successfully")
        except Exception as e:
            logger.error(f"Error in Strategy.start(): {e}\n{traceback.format_exc()}")
            raise

    def on_tick(self):
        """Called on each tick - delegates to parent and checks for refresh."""
        super().on_tick()

        # Check for daily refresh
        if self.config.enable_daily_refresh:
            self._check_daily_refresh()

    def _check_daily_refresh(self) -> None:
        """Check if it's time for daily pair refresh."""
        now = datetime.now(timezone.utc)
        today = now.date()

        if self._last_refresh_date == today:
            return

        refresh_time = now.replace(
            hour=self.config.refresh_hour_utc,
            minute=self.config.refresh_minute_utc,
            second=0,
            microsecond=0,
        )

        if now >= refresh_time:
            logger.info("Starting daily pair refresh")
            self._refresh_pairs()

    def _refresh_pairs(self) -> None:
        """Refresh pairs from database."""
        if not self._pair_loader:
            return

        # Load new pairs (filter unavailable symbols for demo)
        excluded = _DEMO_EXCLUDED_SYMBOLS if _IS_DEMO_MODE else None
        new_pairs = self._pair_loader.load_active_pairs(
            max_pairs=self.config.max_pairs,
            excluded_symbols=excluded
        )

        # Convert to controller ID format for comparison
        new_controller_ids = {self._pair_id_to_controller_id(p.pair_id) for p in new_pairs}
        current_controller_ids = set(self.controllers.keys())

        # Mark removed pairs as exit-only
        removed = current_controller_ids - new_controller_ids
        for controller_id in removed:
            if controller_id not in self._exit_only_pairs:
                self._exit_only_pairs[controller_id] = datetime.now(timezone.utc)
                logger.info(f"Controller {controller_id} marked as exit-only")

        # Add new pairs
        added = new_controller_ids - current_controller_ids
        for pair in new_pairs:
            controller_id = self._pair_id_to_controller_id(pair.pair_id)
            if controller_id in added:
                self._create_controller_for_pair(pair)
                logger.info(f"Added new pair: {pair.pair_id}")

        self._last_refresh_date = datetime.now(timezone.utc).date()

    async def on_stop(self):
        """Called when strategy stops."""
        logger.info("Stopping StatArbMultiStrategy...")

        # Call parent cleanup
        await super().on_stop()

        # End shared logging session
        if self._shared_live_logger:
            try:
                self._shared_live_logger.end_session(
                    reason="strategy_stop",
                    status="stopped",
                )
                logger.info(f"Ended shared logging session: {self._shared_live_logger.session_id}")
            except Exception as e:
                logger.warning(f"Failed to end logging session: {e}")

        # Close database connection
        if self._db:
            try:
                self._db.close()
            except Exception:
                pass

        logger.info("StatArbMultiStrategy stopped")

    def format_status(self) -> str:
        """Format status for Hummingbot's status command."""
        # Get base status from parent
        base_status = super().format_status()

        # Add our custom info
        lines = [
            "",
            "=" * 50,
            "Multi-Pair Statistical Arbitrage (V2)",
            "=" * 50,
            f"Mode: {self.config.trading_mode}",
            f"Capital: ${self.config.total_capital_quote}",
            f"Controllers: {len(self.controllers)} active, "
            f"{len(self._exit_only_pairs)} exit-only",
            f"Last Refresh: {self._last_refresh_date}",
            "",
        ]

        for controller_id, controller in self.controllers.items():
            is_exit = "[EXIT] " if controller_id in self._exit_only_pairs else ""
            try:
                data = controller.processed_data
                zscore = float(data.get("zscore", 0))
                signal = controller._signal_name(data.get("signal", 0))
                lines.append(f"  {is_exit}{controller_id}: z={zscore:+.2f}, sig={signal}")
            except Exception:
                lines.append(f"  {is_exit}{controller_id}: (no data)")

        return base_status + "\n".join(lines)
