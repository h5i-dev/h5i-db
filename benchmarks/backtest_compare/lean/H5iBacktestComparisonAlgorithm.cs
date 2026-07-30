using System.Diagnostics;
using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.Data;

namespace H5i.BacktestCompare;

public sealed class H5iBacktestComparisonAlgorithm : QCAlgorithm
{
    private readonly Stopwatch _measured = new();
    private Symbol? _symbol;
    private int _events;
    private int _orders;
    private int _eventTarget;
    private int _signalTarget;
    private int _spacing;

    public override void Initialize()
    {
        SetTimeZone(TimeZones.Utc);
        SetStartDate(2020, 1, 6);
        SetEndDate(2020, 1, 8);
        SetCash(1_000_000);
        _eventTarget = GetParameter("event-count", 200_000);
        _signalTarget = GetParameter("signal-count", 200);
        _spacing = Math.Max(_eventTarget / Math.Max(_signalTarget, 1), 1);
        _symbol = AddForex(
            "EURUSD",
            Resolution.Second,
            Market.Oanda,
            fillForward: false
        ).Symbol;
    }

    public override void OnData(Slice data)
    {
        if (_symbol is null || !data.QuoteBars.ContainsKey(_symbol))
        {
            return;
        }
        if (_events == 0)
        {
            _measured.Start();
        }
        _events++;
        if (_orders < _signalTarget && _events == 2 + _orders * _spacing)
        {
            MarketOrder(_symbol, _orders % 2 == 0 ? 1 : -1);
            _orders++;
        }
    }

    public override void OnEndOfAlgorithm()
    {
        _measured.Stop();
        Debug(
            "H5I_BACKTEST_COMPARE "
            + $"events={_events} orders={_orders} engine_ms={_measured.Elapsed.TotalMilliseconds:F6}"
        );
    }
}
