"""The bar on-ramp and the JSON feeds, with publication time taken seriously.

The recurring hazard in both is the same and it is not a crash: a value stamped
at the instant it *describes* rather than the instant it became readable
backtests beautifully and loses money live. So most of what is checked here is
when a row says it became knowable.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

import h5i_db
from h5i_db import venues

DAY_NS = 86_400 * 1_000_000_000
MINUTE_NS = 60 * 1_000_000_000


def _ohlcv_table():
    return pa.table(
        {
            "Date": ["2026-01-02", "2026-01-05"],
            "Open": [10.0, 11.0],
            "High": [12.0, 13.0],
            "Low": [9.0, 10.5],
            "Close": [11.0, 12.5],
            "Volume": [100.0, 250.0],
        }
    )


def test_a_bar_becomes_knowable_when_its_interval_closes():
    bars = venues.bars_from_table(_ohlcv_table(), instrument_id="AAPL")
    init = pa.compute.cast(bars.column("ts_init"), pa.int64()).to_pylist()
    event = pa.compute.cast(bars.column("ts_event"), pa.int64()).to_pylist()

    # ts_event is the session the prices happened in; ts_init is the instant a
    # strategy could first have read them, one interval later. Stamping both at
    # the open would let a strategy trade a bar that has not formed yet.
    assert all(i - e == DAY_NS for i, e in zip(init, event))
    assert all(i > e for i, e in zip(init, event))
    # Column lookup is case-insensitive: "Date" and "date" are the same export.
    assert bars.column("open").to_pylist() == [10.0, 11.0]
    assert bars.schema == venues.BARS_SCHEMA


def test_a_layout_that_cannot_say_when_a_bar_closed_is_refused():
    # There is no safe default for this, so it is a construction error rather
    # than a silently wrong timestamp.
    with pytest.raises(ValueError, match="knowable"):
        venues.BarLayout(name="nameless", close_time_column=None, interval=None)


def test_an_inclusive_vendor_close_time_is_advanced_to_the_real_boundary():
    # Binance writes the last instant *inside* the bar (…59.999999), one tick
    # short of the boundary. Left alone that is a one-tick look-ahead, and the
    # bars would not tile: each close would fall short of the next open.
    klines = pa.table(
        {
            "open_time": [1_781_481_600_000_000, 1_781_481_660_000_000],
            "open": [1.0, 2.0],
            "high": [3.0, 4.0],
            "low": [0.5, 1.5],
            "close": [2.5, 3.5],
            "volume": [10.0, 20.0],
            "close_time": [1_781_481_659_999_999, 1_781_481_719_999_999],
            "quote_volume": [0.0, 0.0],
            "trades": [1, 2],
            "taker_buy_base": [0.0, 0.0],
            "taker_buy_quote": [0.0, 0.0],
            "ignore": [0, 0],
        }
    )
    bars = venues.bars_from_table(
        klines, instrument_id="BTCUSDT", layout=venues.BINANCE_KLINES_LAYOUT
    )
    init = pa.compute.cast(bars.column("ts_init"), pa.int64()).to_pylist()
    event = pa.compute.cast(bars.column("ts_event"), pa.int64()).to_pylist()

    assert all(i - e == MINUTE_NS for i, e in zip(init, event))
    # One bar's close is the next bar's open: no gap, no overlap.
    assert init[0] == event[1]


def test_a_pandas_frame_keeps_its_index_as_the_time_column():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {"open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [7.0]},
        index=pd.to_datetime(["2026-03-02"]),
    )
    # This is the shape `yfinance.download()` returns, so requiring a manual
    # reset_index() would be a papercut on the most common path there is.
    bars = venues.bars_from_dataframe(frame, instrument_id="MSFT")
    assert bars.num_rows == 1
    assert bars.column("close").to_pylist() == [1.5]


def test_bars_derived_from_trades_close_at_the_interval_end():
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(str(Path(tmp) / "m.db"), create=True)
        venues.ensure_tables(db, ["trades"])
        base = 1_777_000_000 * 1_000_000_000
        trades = pa.table(
            {
                "ts_init": pa.array([base, base + 10, base + 20, base + MINUTE_NS], pa.timestamp("ns")),
                "ts_event": pa.array([base, base + 10, base + 20, base + MINUTE_NS], pa.timestamp("ns")),
                "instrument_id": ["M"] * 4,
                "outcome": pa.array([0, 0, 0, 0], pa.uint16()),
                "price": [0.50, 0.60, 0.55, 0.70],
                "size": [10.0, 5.0, 2.0, 1.0],
                "aggressor": [None] * 4,
                "trade_id": [None] * 4,
                "source_vendor": ["t"] * 4,
            },
            schema=venues.TRADES_SCHEMA,
        )
        db.append("trades", trades)

        report = venues.bars_from_trades(db, interval="1m")
        bars = db.sql("SELECT * FROM bars ORDER BY ts_event").to_pandas()

        assert report.tables["bars"].rows == 2
        first = bars.iloc[0]
        # open and close follow trade order, not price order.
        assert (first.open, first.high, first.low, first.close) == (0.50, 0.60, 0.50, 0.55)
        assert first.volume == 17.0
        # The aggregate is not knowable until the minute ends.
        assert (bars.ts_init - bars.ts_event).dt.total_seconds().tolist() == [60.0, 60.0]
        # An interval with no trade produces no bar: a hole stays a hole rather
        # than becoming a flat candle that implies a quiet market.
        assert len(bars) == 2


def test_a_manifold_bet_is_priced_at_what_it_actually_paid():
    bets = [
        # 60 mana for 100 shares is 0.60 a share, whatever the AMM's marginal
        # price ended up at. Pricing at probAfter would overstate the cost.
        {"contractId": "c1", "createdTime": 1_777_000_000_000, "amount": 60.0,
         "shares": 100.0, "outcome": "YES", "probAfter": 0.75, "id": "b1"},
        # A redemption moves no risk between participants.
        {"contractId": "c1", "createdTime": 1_777_000_001_000, "amount": 5.0,
         "shares": 5.0, "outcome": "NO", "isRedemption": True, "id": "b2"},
        # An unfilled limit order is not a print.
        {"contractId": "c1", "createdTime": 1_777_000_002_000, "amount": 0.0,
         "shares": 0.0, "outcome": "YES", "isFilled": False, "id": "b3"},
    ]
    counters: dict[str, int] = {}
    trades = venues.manifold_trades_from_json(bets, skipped=counters)

    assert trades.num_rows == 1
    assert trades.column("price").to_pylist() == [0.60]
    assert trades.column("size").to_pylist() == [100.0]
    assert trades.column("outcome").to_pylist() == [0]
    assert counters["redemption"] == 1 and counters["unfilled"] == 1


def test_manifold_resolutions_distinguish_a_winner_from_a_split():
    settled = 1_777_000_000_000
    skipped: list[dict] = []
    specs = venues.manifold_markets_from_json(
        [
            {"id": "won", "outcomeType": "BINARY", "isResolved": True,
             "resolution": "YES", "resolutionTime": settled},
            {"id": "void", "outcomeType": "BINARY", "isResolved": True,
             "resolution": "CANCEL", "resolutionTime": settled},
            {"id": "part", "outcomeType": "BINARY", "isResolved": True,
             "resolution": "MKT", "resolutionProbability": 0.3, "resolutionTime": settled},
            # Resolved but with no instant to settle against: skipped, because
            # inventing one would move every payout that depends on it.
            {"id": "when", "outcomeType": "BINARY", "isResolved": True, "resolution": "YES"},
        ],
        skipped=skipped,
    )
    by_id = {spec.instrument_id: spec for spec in specs}
    assert "when" not in by_id
    assert skipped[0]["reason"] == "resolved_without_resolution_time"
    assert by_id["won"].winner_outcome == 0
    # A void refunds both sides at cost; recording it as a winner would be
    # wrong by the full notional on each side.
    assert by_id["void"].voided is True and by_id["void"].winner_outcome is None
    assert by_id["part"].payouts == (0.3, 0.7)


def test_an_unsupported_manifold_market_is_reported_not_approximated():
    skipped: list[dict] = []
    specs = venues.manifold_markets_from_json(
        [{"id": "mc", "outcomeType": "MULTIPLE_CHOICE"}], skipped=skipped
    )
    assert specs == []
    assert skipped[0]["reason"] == "unsupported_outcome_type"


def test_a_published_series_is_readable_only_after_it_is_published():
    rows = [("2026-07-27", "4.21"), ("2026-07-28", "."), ("2026-07-29", "4.25")]
    table = venues.references_from_series(
        rows, instrument_id="DGS10", published_after="1d"
    )
    init = pa.compute.cast(table.column("ts_init"), pa.int64()).to_pylist()
    event = pa.compute.cast(table.column("ts_event"), pa.int64()).to_pylist()

    # Monday's rate is not readable on Monday, so ts_init trails ts_event by
    # the publisher's lag rather than sitting on top of it.
    assert all(i - e == DAY_NS for i, e in zip(init, event))
    # A missing observation is a hole. Read as zero it would be a rate cut.
    assert table.num_rows == 2
    assert table.column("mark").to_pylist() == [4.21, 4.25]
    assert table.column("oracle").null_count == 2


def test_a_series_with_no_stated_publication_lag_is_refused():
    with pytest.raises(ValueError):
        venues.references_from_series(
            [("2026-07-27", "1.0")], instrument_id="X", published_after=None
        )
