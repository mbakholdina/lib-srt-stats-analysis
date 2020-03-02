# lib-srt-stats-analysis

A library designed to work with SRT core statistics and `tshark` datasets.

# Getting Started

## Requirements

* python 3.6+

Please follow the [instructions](https://github.com/mbakholdina/lib-tcpdump-processing#getting-started) to get started with library installation and usage.

# Executable scripts

```
venv/bin/python -m srt_stats_analysis.join_stats
```

# ToDo

1. Rename the library to lib-srt-stats.
1. Align SRT and tshark datasets.
1. Add remain features in align_srt_stats function.
1. Script with plotting result dataframes.
2. SRT statistics documentation:
    - Note regarding the first data point (the time of connection),
    - Note regarding aggregated and not aggregated statistics,
    - Correct mbpsBandwidth description
    - Note regarding relative time and new absolute time.
3. srt-live-transmit:
    - Ticket: Add absolute time points plus first data point when connection has been established
4. Test caller-rcv and listener-sender use case.
5. Extract initial RTT from handshakes and adjust receiver and sender clocks.
6. Experiment with shifting receiver statistics by RTT/2 (as a start, we can shift by initial RTT calculated from handshakes).
7. Extract RTT estimation from acknoledgement packets and experiment with shifting receiver statistics by RTT/2.

Do not forget about issues found:
- Missing ACKACK - write an issue on github.
- mbpsBandwidth - some strange behaviour at the beginning.
- Initial RTT estimation is equal to 100ms and then slowly converges to the real value 60-65ms. Think about proper initial RTT value.