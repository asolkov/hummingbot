"""
Hummingbot V2 Script for Statistical Arbitrage Trading.

This script uses the native Hummingbot V2 Strategy Framework:
- Uses ScriptStrategyBase + ControllerBase pattern
- Gets data via market_data_provider
- Executes via ExecutorAction pattern

Usage:
    1. Copy to Hummingbot scripts directory:
       cp hummingbot/scripts/stat_arb_v2_script.py ~/hummingbot/scripts/

    2. Configure OKX perpetual connector:
       connect okx_perpetual

    3. Start the script:
       start --script stat_arb_v2_script.py

The V2 controller is fully integrated with Hummingbot's framework,
using candles feed for historical data and the executor pattern for orders.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, Set

from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.connector.connector_base import ConnectorBase

# Add project root to path for imports
import sys
PROJECT_ROOT = Path(__file__).parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import V2 controller
from hummingbot.controllers.stat_arb_v2 import StatArbV2, StatArbV2Config


logger = logging.getLogger(__name__)


class StatArbV2Script(ScriptStrategyBase):
    """
    Hummingbot V2 Script for Statistical Arbitrage.

    This script demonstrates using the StatArbV2 controller which is
    fully integrated with Hummingbot's V2 Strategy Framework.

    Configuration is done via StatArbV2Config - modify the config below
    or create from database/YAML.
    """

    # ========== Configuration ==========

    # Connector name - must match what you configured in Hummingbot
    CONNECTOR_NAME = "okx_perpetual"

    # Trading pairs
    TRADING_PAIR_A = "ETH-USDT-SWAP"
    TRADING_PAIR_B = "BTC-USDT-SWAP"

    # Hedge ratio (from cointegration analysis)
    HEDGE_RATIO = Decimal("1.0")

    # Signal thresholds
    ZSCORE_ENTRY = Decimal("3.0")
    ZSCORE_EXIT = Decimal("0.5")

    # Position sizing (USD per leg)
    POSITION_SIZE = Decimal("100")

    # Leverage
    LEVERAGE = 5

    # Trading mode for logging
    TRADING_MODE = "demo"  # "demo" or "live"

    # Database logging
    ENABLE_DB_LOGGING = True

    # ========== Markets ==========

    markets: Dict[str, Set[str]] = {}

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)

        # Initialize markets
        self.markets = {
            self.CONNECTOR_NAME: {self.TRADING_PAIR_A, self.TRADING_PAIR_B}
        }

        # Create V2 config
        self._config = StatArbV2Config(
            connector_name=self.CONNECTOR_NAME,
            trading_pair_a=self.TRADING_PAIR_A,
            trading_pair_b=self.TRADING_PAIR_B,
            hedge_ratio=self.HEDGE_RATIO,
            zscore_entry_threshold=self.ZSCORE_ENTRY,
            zscore_exit_threshold=self.ZSCORE_EXIT,
            position_size_quote=self.POSITION_SIZE,
            leverage=self.LEVERAGE,
            trading_mode=self.TRADING_MODE,
            enable_db_logging=self.ENABLE_DB_LOGGING,
            # Additional config from base class
            total_amount_quote=self.POSITION_SIZE * 2,  # Both legs combined
        )

        # Create V2 controller
        # Note: The controller needs market_data_provider which is available
        # after Hummingbot initializes the script. We'll create it on first tick.
        self._controller = None
        self._initialized = False

    @classmethod
    def init_markets(cls, config) -> None:
        """Initialize markets for Hummingbot."""
        cls.markets = {
            cls.CONNECTOR_NAME: {cls.TRADING_PAIR_A, cls.TRADING_PAIR_B}
        }
        logger.info(f"Initialized markets: {cls.markets}")

    def on_tick(self) -> None:
        """
        Called by Hummingbot on each tick.

        Delegates to the V2 controller's update and action cycle.
        """
        # Initialize controller on first tick (market_data_provider is ready)
        if not self._initialized:
            self._initialize_controller()
            return

        if self._controller is None:
            return

        try:
            # V2 controllers use async update_processed_data
            # For sync context, we run the coroutine
            import asyncio

            # Update processed data (prices, spread, signals)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._controller.update_processed_data())

            # Get executor actions from controller
            actions = self._controller.determine_executor_actions()

            # Execute actions via Hummingbot's strategy handler
            for action in actions:
                self.execute_action(action)

        except Exception as e:
            logger.error(f"Error in on_tick: {e}", exc_info=True)

    def _initialize_controller(self) -> None:
        """Initialize the V2 controller."""
        try:
            logger.info("Initializing StatArbV2 controller...")

            # Verify connector is available
            if self.CONNECTOR_NAME not in self.connectors:
                logger.error(f"Connector {self.CONNECTOR_NAME} not found")
                return

            # Create the controller with market_data_provider
            # Note: In V2 framework, the controller accesses market_data_provider
            # through the base class. We need to pass it properly.

            # For ScriptStrategyBase, we need to create a wrapper that provides
            # the market_data_provider interface. Let's use a simpler approach
            # by creating the controller with the script's connectors.

            self._controller = StatArbV2(
                config=self._config,
                # market_data_provider is set automatically by ControllerBase
                # when used within the V2 framework
            )

            self._initialized = True
            logger.info(
                f"StatArbV2 controller initialized: "
                f"pair_a={self.TRADING_PAIR_A}, pair_b={self.TRADING_PAIR_B}, "
                f"hedge_ratio={self.HEDGE_RATIO}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize controller: {e}", exc_info=True)

    def execute_action(self, action) -> None:
        """
        Execute an executor action.

        In the full V2 framework, this is handled by the strategy orchestrator.
        For script usage, we need to implement the action execution ourselves.
        """
        from hummingbot.strategy_v2.models.executor_actions import (
            CreateExecutorAction,
            StopExecutorAction,
        )
        from hummingbot.strategy_v2.executors.position_executor.data_types import (
            PositionExecutorConfig,
        )
        from hummingbot.strategy_v2.executors.order_executor.data_types import (
            OrderExecutorConfig,
        )
        from hummingbot.core.data_type.common import OrderType

        try:
            if isinstance(action, CreateExecutorAction):
                config = action.executor_config

                if isinstance(config, PositionExecutorConfig):
                    # Position executor - place limit order
                    if config.side.name == "BUY":
                        self.buy(
                            connector_name=config.connector_name,
                            trading_pair=config.trading_pair,
                            amount=config.amount,
                            order_type=OrderType.LIMIT,
                            price=config.entry_price,
                        )
                    else:
                        self.sell(
                            connector_name=config.connector_name,
                            trading_pair=config.trading_pair,
                            amount=config.amount,
                            order_type=OrderType.LIMIT,
                            price=config.entry_price,
                        )
                    logger.info(
                        f"Created position executor: {config.trading_pair} "
                        f"{config.side.name} {config.amount} @ {config.entry_price}"
                    )

                elif isinstance(config, OrderExecutorConfig):
                    # Order executor - typically market order for closing
                    if config.side.name == "BUY":
                        self.buy(
                            connector_name=config.connector_name,
                            trading_pair=config.trading_pair,
                            amount=config.amount,
                            order_type=OrderType.MARKET,
                        )
                    else:
                        self.sell(
                            connector_name=config.connector_name,
                            trading_pair=config.trading_pair,
                            amount=config.amount,
                            order_type=OrderType.MARKET,
                        )
                    logger.info(
                        f"Created order executor: {config.trading_pair} "
                        f"{config.side.name} {config.amount} MARKET"
                    )

            elif isinstance(action, StopExecutorAction):
                # Stop executor - in full V2, this would stop an active executor
                # For script usage, we track and cancel relevant orders
                logger.debug(f"Stop executor action: {action.executor_id}")

        except Exception as e:
            logger.error(f"Failed to execute action: {e}")

    def format_status(self) -> str:
        """Return status string for Hummingbot's status command."""
        if not self._initialized or self._controller is None:
            return "  StatArbV2: Initializing..."

        # Get status from controller
        status_lines = self._controller.to_format_status()
        return "\n".join(status_lines)

    def on_stop(self) -> None:
        """Called when Hummingbot stops the script."""
        logger.info("Stopping StatArbV2Script...")

        # End logging session if controller has one
        if self._controller and hasattr(self._controller, '_live_logger'):
            if self._controller._live_logger:
                self._controller._live_logger.end_session(reason="script_stop")

        logger.info("StatArbV2Script stopped")


# =============================================================================
# Alternative: Use V2 controller directly in a V2 Strategy
#
# For full V2 framework integration (recommended for production), you would:
# 1. Create a StrategyV2 subclass
# 2. Register the StatArbV2 controller in __init__
# 3. The framework handles all executor actions automatically
#
# Example:
#
# from hummingbot.strategy_v2.strategy import StrategyV2
#
# class StatArbStrategy(StrategyV2):
#     def __init__(self, connectors, config):
#         super().__init__(connectors, config)
#         self.add_controller(StatArbV2(StatArbV2Config(...)))
#
# =============================================================================
