import asyncio
from typing import Dict, List, Set

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class DynamicOrderbookDemo(ScriptStrategyBase):
    """
    Demo script showing dynamic order book initialization and removal.

    The script uses one connector for the strategy (markets) but can display
    order books from a different exchange (order_book_exchange).

    Timeline:
    - Starts with initial_trading_pairs
    - Adds pairs from add_trading_pairs after add_pairs_delay seconds
    - Removes pairs from remove_trading_pairs after remove_pairs_delay seconds

    Order book data is displayed in format_status() (use `status` command to view).

    Usage: start --script dynamic_orderbook_demo.py
    """

    # ===========================================
    # CONFIGURATION - Modify these parameters
    # ===========================================

    # Exchange to use for order book data (can be different from trading exchange)
    # Options: "binance", "bybit", "kraken", "gate_io", "kucoin", etc.
    order_book_exchange: str = "kraken"

    # Trading pairs to add dynamically
    add_trading_pairs: Set[str] = {"SOL-USDT", "ETH-USDT"}

    # Trading pairs to remove dynamically (must be subset of add_trading_pairs)
    remove_trading_pairs: Set[str] = {"SOL-USDT", "ETH-USDT"}

    # Timing configuration (in seconds)
    add_pairs_delay: float = 10.0  # Add pairs after this many seconds
    remove_pairs_delay: float = 25.0  # Remove pairs after this many seconds

    # Display configuration
    ORDER_BOOK_DEPTH: int = 5  # Number of levels to display
    HISTOGRAM_RANGE_BPS: int = 50  # ±50 bps from mid price
    CHART_HEIGHT: int = 12  # Height of depth chart in rows
    markets = {"binance_paper_trade": {"BTC-USDT"}}

    # ===========================================
    # Strategy setup - uses order_book_exchange
    # ===========================================
    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self._start_timestamp = None
        self._pairs_added: Set[str] = set()
        self._pairs_removed: Set[str] = set()
        self._market_data_provider = MarketDataProvider(connectors)

    def on_tick(self):
        if self._start_timestamp is None:
            self._start_timestamp = self.current_timestamp

        elapsed = self.current_timestamp - self._start_timestamp

        # Add trading pairs after add_pairs_delay
        if elapsed >= self.add_pairs_delay:
            for pair in self.add_trading_pairs:
                if pair not in self._pairs_added:
                    self._pairs_added.add(pair)
                    self.logger().info(f">>> ADDING {pair} ORDER BOOK <<<")
                    asyncio.create_task(self._add_trading_pair(pair))

        # Remove trading pairs after remove_pairs_delay
        if elapsed >= self.remove_pairs_delay:
            for pair in self.remove_trading_pairs:
                if pair not in self._pairs_removed and pair in self._pairs_added:
                    self._pairs_removed.add(pair)
                    self.logger().info(f">>> REMOVING {pair} ORDER BOOK <<<")
                    asyncio.create_task(self._remove_trading_pair(pair))

    def format_status(self) -> str:
        """
        Displays order book information for all tracked trading pairs.
        Use the `status` command to view this output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []
        elapsed = self.current_timestamp - self._start_timestamp if self._start_timestamp else 0

        lines.append("\n" + "=" * 80)
        lines.append(f"  DYNAMIC ORDER BOOK DEMO | Exchange: {self.order_book_exchange} | Elapsed: {elapsed:.1f}s")
        lines.append("=" * 80)

        # Timeline status
        lines.append("\n  Timeline:")
        add_pairs_str = ", ".join(self.add_trading_pairs) if self.add_trading_pairs else "None"
        remove_pairs_str = ", ".join(self.remove_trading_pairs) if self.remove_trading_pairs else "None"
        all_added = self.add_trading_pairs <= self._pairs_added
        all_removed = self.remove_trading_pairs <= self._pairs_removed

        lines.append(
            f"    [{'✓' if all_added else '○'}] {self.add_pairs_delay:.0f}s - Add {add_pairs_str}"
            + (" (added)" if all_added else "")
        )
        lines.append(
            f"    [{'✓' if all_removed else '○'}] {self.remove_pairs_delay:.0f}s - Remove {remove_pairs_str}"
            + (" (removed)" if all_removed else "")
        )

        # Check if the non-trading connector has been started
        connector = self._market_data_provider.get_connector_with_fallback(self.order_book_exchange)
        is_started = self._market_data_provider._non_trading_connectors_started.get(
            self.order_book_exchange, False
        ) if self.order_book_exchange not in self._market_data_provider.connectors else True

        if not is_started:
            lines.append(f"\n  Waiting for first trading pair to be added...")
            lines.append(f"  (Order book connector will start at {self.add_pairs_delay:.0f}s)")
            lines.append("\n" + "=" * 80)
            return "\n".join(lines)

        # Get tracked pairs from order book tracker
        tracker = connector.order_book_tracker
        tracked_pairs = list(tracker.order_books.keys())
        lines.append(f"\n  Tracked Pairs: {tracked_pairs}")

        if not tracked_pairs:
            lines.append("\n  No order books currently tracked.")
            lines.append("\n" + "=" * 80)
            return "\n".join(lines)

        # Display order book for each pair
        for pair in tracked_pairs:
            lines.append("\n" + "-" * 80)
            lines.extend(self._format_order_book(connector, pair))

        lines.append("\n" + "=" * 80)
        return "\n".join(lines)

    def _format_order_book(self, connector, trading_pair: str) -> List[str]:
        """Format order book data for a single trading pair with horizontal depth chart."""
        lines = []

        try:
            ob = connector.order_book_tracker.order_books.get(trading_pair)
            if ob is None:
                lines.append(f"  {trading_pair}: Order book not yet initialized...")
                return lines

            bids_df, asks_df = ob.snapshot

            if len(bids_df) == 0 or len(asks_df) == 0:
                lines.append(f"  {trading_pair}: Order book empty or initializing...")
                return lines

            # Calculate market metrics
            best_bid = float(bids_df.iloc[0].price)
            best_ask = float(asks_df.iloc[0].price)
            spread = best_ask - best_bid
            spread_pct = (spread / best_bid) * 100
            mid_price = (best_bid + best_ask) / 2

            # Header with market info
            lines.append(f"  {trading_pair}")
            lines.append(f"  Mid: {mid_price:.4f} | Spread: {spread:.4f} ({spread_pct:.4f}%)")
            lines.append("")

            # Prepare order book display
            depth = min(self.ORDER_BOOK_DEPTH, len(bids_df), len(asks_df))

            # Order book table
            lines.append(f"  {'Bid Size':>12} {'Bid Price':>14} │ {'Ask Price':<14} {'Ask Size':<12}")
            lines.append(f"  {'-' * 12} {'-' * 14} │ {'-' * 14} {'-' * 12}")

            for i in range(depth):
                bid_price = float(bids_df.iloc[i].price)
                bid_size = float(bids_df.iloc[i].amount)
                ask_price = float(asks_df.iloc[i].price)
                ask_size = float(asks_df.iloc[i].amount)
                lines.append(f"  {bid_size:>12.4f} {bid_price:>14.4f} │ {ask_price:<14.4f} {ask_size:<12.4f}")

            # Total volume at displayed levels
            total_bid_vol = float(bids_df.iloc[:depth]['amount'].sum())
            total_ask_vol = float(asks_df.iloc[:depth]['amount'].sum())
            lines.append(f"  {'-' * 12} {'-' * 14} │ {'-' * 14} {'-' * 12}")
            lines.append(f"  {'Total:':>12} {total_bid_vol:>14.4f} │ {total_ask_vol:<14.4f}")

            # Add horizontal depth chart below
            lines.extend(self._build_horizontal_depth_chart(bids_df, asks_df, mid_price))

        except Exception as e:
            lines.append(f"  {trading_pair}: Error - {e}")

        return lines

    def _build_horizontal_depth_chart(self, bids_df, asks_df, mid_price: float) -> List[str]:
        """
        Build ASCII depth chart with bps on X-axis and volume on Y-axis.
        1 bar per bps, bids on left, asks on right.
        """
        lines = []
        num_buckets = self.HISTOGRAM_RANGE_BPS  # 1 bucket per bps
        range_decimal = self.HISTOGRAM_RANGE_BPS / 10000
        bucket_size_decimal = range_decimal / num_buckets

        # Aggregate bid volume into buckets (1 per bps, from mid going down)
        bid_buckets = []
        for i in range(num_buckets):
            bucket_upper = mid_price * (1 - i * bucket_size_decimal)
            bucket_lower = mid_price * (1 - (i + 1) * bucket_size_decimal)
            mask = (bids_df['price'] <= bucket_upper) & (bids_df['price'] > bucket_lower)
            vol = float(bids_df[mask]['amount'].sum()) if mask.any() else 0
            bid_buckets.append(vol)

        # Aggregate ask volume into buckets (1 per bps, from mid going up)
        ask_buckets = []
        for i in range(num_buckets):
            bucket_lower = mid_price * (1 + i * bucket_size_decimal)
            bucket_upper = mid_price * (1 + (i + 1) * bucket_size_decimal)
            mask = (asks_df['price'] >= bucket_lower) & (asks_df['price'] < bucket_upper)
            vol = float(asks_df[mask]['amount'].sum()) if mask.any() else 0
            ask_buckets.append(vol)

        # Find max volume for scaling
        all_vols = bid_buckets + ask_buckets
        max_vol = max(all_vols) if all_vols and max(all_vols) > 0 else 1

        # Calculate totals and imbalance
        total_bid_vol = sum(bid_buckets)
        total_ask_vol = sum(ask_buckets)
        total_vol = total_bid_vol + total_ask_vol
        imbalance_pct = ((total_bid_vol - total_ask_vol) / total_vol * 100) if total_vol > 0 else 0
        imbalance_str = f"+{imbalance_pct:.1f}%" if imbalance_pct >= 0 else f"{imbalance_pct:.1f}%"

        # Calculate bar heights
        chart_height = self.CHART_HEIGHT
        bid_heights = [int((v / max_vol) * chart_height) for v in bid_buckets]
        ask_heights = [int((v / max_vol) * chart_height) for v in ask_buckets]

        # Reverse bids so furthest from mid is on left
        bid_heights_display = list(reversed(bid_heights))

        # Header with summary
        lines.append("")
        lines.append(f"  Depth Chart (±{self.HISTOGRAM_RANGE_BPS} bps)")
        lines.append(f"  Bids: {total_bid_vol:,.2f}  |  Asks: {total_ask_vol:,.2f}  |  Imbalance: {imbalance_str}")
        lines.append("")

        # Build chart row by row from top to bottom
        for row in range(chart_height, 0, -1):
            row_chars = []

            # Bid bars (left side)
            for h in bid_heights_display:
                if h >= row:
                    row_chars.append("█")
                else:
                    row_chars.append(" ")

            # Center divider
            row_chars.append("│")

            # Ask bars (right side)
            for h in ask_heights:
                if h >= row:
                    row_chars.append("█")
                else:
                    row_chars.append(" ")

            lines.append("  " + "".join(row_chars))

        # X-axis line
        lines.append("  " + "─" * num_buckets + "┴" + "─" * num_buckets)

        # X-axis labels (sparse - every 10 bps)
        label_interval = 10
        # Build label line with proper spacing
        bid_label_line = [" "] * num_buckets
        ask_label_line = [" "] * num_buckets

        # Place bid labels (negative values, from left to right: -50, -40, -30, -20, -10)
        for bps in range(self.HISTOGRAM_RANGE_BPS, 0, -label_interval):
            pos = self.HISTOGRAM_RANGE_BPS - bps  # Position from left
            label = f"-{bps}"
            # Center the label at this position
            start = max(0, pos - len(label) // 2)
            for j, ch in enumerate(label):
                if start + j < num_buckets:
                    bid_label_line[start + j] = ch

        # Place ask labels (positive values, from left to right: 10, 20, 30, 40, 50)
        for bps in range(label_interval, self.HISTOGRAM_RANGE_BPS + 1, label_interval):
            pos = bps - 1  # Position from left (0-indexed)
            label = f"+{bps}"
            start = max(0, pos - len(label) // 2)
            for j, ch in enumerate(label):
                if start + j < num_buckets:
                    ask_label_line[start + j] = ch

        lines.append("  " + "".join(bid_label_line) + "0" + "".join(ask_label_line) + "  (bps)")

        return lines

    async def _add_trading_pair(self, trading_pair: str):
        """Add a trading pair to the order book tracker."""
        try:
            success = await self._market_data_provider.initialize_order_book(
                self.order_book_exchange, trading_pair
            )
            if not success:
                self.logger().error(f"Failed to add {trading_pair} to order book tracker")
                return

            self.logger().info(f"Successfully added {trading_pair}!")

        except Exception as e:
            self.logger().exception(f"Error adding {trading_pair}: {e}")

    async def _remove_trading_pair(self, trading_pair: str):
        """Remove a trading pair from the order book tracker."""
        try:
            success = await self._market_data_provider.remove_order_book(
                self.order_book_exchange, trading_pair
            )
            if not success:
                self.logger().error(f"Failed to remove {trading_pair} from order book tracker")
                return

            self.logger().info(f"Successfully removed {trading_pair}!")

        except Exception as e:
            self.logger().exception(f"Error removing {trading_pair}: {e}")
