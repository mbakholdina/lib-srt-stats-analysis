""" TODO """
import pathlib

# import matplotlib.pyplot as plt
import pandas as pd
# import plotly.graph_objects as go
# import seaborn as sns

from tcpdump_processing import convert, extract_packets


def align_srt_stats(snd_stats_path: str, rcv_stats_path: str):
    # Set the list of SRT statistics features to analyze
    SND_FEATURES = [
        # 'pktFlowWindow',
        # 'pktCongestionWindow',
        # 'pktFlightSize',
        'msRTT',
        'mbpsBandwidth',
        'pktSent',
        'pktSndLoss',
        # 'pktSndDrop',
        # 'pktRetrans',
        # 'byteSent',
        # 'byteSndDrop',
        # 'mbpsSendRate',
        # 'usPktSndPeriod',
    ]

    RCV_FEATURES = [
        'msRTT',
        'mbpsBandwidth',
        'pktRecv',
        'pktRcvLoss',
        # 'pktRcvDrop',
        # 'pktRcvRetrans',
        # 'pktRcvBelated',
        # 'byteRecv',
        # 'byteRcvLoss',
        # 'byteRcvDrop',
        # 'mbpsRecvRate',
    ]

    # Load SRT statistics from sender and receiver side to dataframes 
    # snd_stats and rcv_stats respectively and extract features of interest
    snd_stats = pd.read_csv(snd_stats_path, index_col='Timepoint', parse_dates=True)
    rcv_stats = pd.read_csv(rcv_stats_path, index_col='Timepoint', parse_dates=True)
    snd_stats = snd_stats[SND_FEATURES]
    rcv_stats = rcv_stats[RCV_FEATURES]

    # Convert timezones to UTC+0
    snd_stats.index = snd_stats.index.tz_convert(None)
    rcv_stats.index = rcv_stats.index.tz_convert(None)

    # print('Sender statisitcs')
    # print(snd_stats.head(10))
    # print(snd_stats.tail(10))
    # print('Receiver statistics')
    # print(rcv_stats.head(10))
    # print(rcv_stats.tail(10))

    # TODO: Adjust clocks

    # Combine sender and receiver datasets into stats dataframe
    snd_stats = snd_stats.add_suffix('_snd')
    rcv_stats = rcv_stats.add_suffix('_rcv')
    snd_stats['isSender'] = True
    stats = snd_stats.join(rcv_stats, how='outer')
    stats['isSender'] = stats['isSender'].fillna(False)

    # Further we will use sender time to align the stats from 
    # receiver and sender
    # To do so, first we cut the time points on top and at the bottom 
    # of stats dataframe where statistics was collected only 
    # on receiver or sender side
    start_timestamp = max(snd_stats.index[0], rcv_stats.index[0])
    end_timestamp = min(snd_stats.index[-1], rcv_stats.index[-1])
    stats = stats[(stats.index >= start_timestamp) & (stats.index <= end_timestamp)]

    # Second, we check that the first and the last timepoints are both
    # sender timepoints. If not, drop them.
    if not stats['isSender'][0]:
        stats = stats[1:]

    if not stats['isSender'][-1]:
        stats = stats[:-1]
    
    # print(stats.head())
    # print(stats.tail())

    # Do linear interpolation for features where applicable
    stats['msRTT_rcv'] = stats['msRTT_rcv'].interpolate()
    stats['msRTT_rcv'] = stats['msRTT_rcv'].fillna(stats['msRTT_rcv'][1])

    stats['mbpsBandwidth_rcv'] = stats['mbpsBandwidth_rcv'].interpolate()
    stats['mbpsBandwidth_rcv'] = stats['mbpsBandwidth_rcv'].fillna(stats['mbpsBandwidth_rcv'][1])

    # The rest statistics is the aggredated statistics, so we will apply
    # special technique to align the frames
    stats['timeDiff'] = stats.index.to_series().diff().fillna(pd.to_timedelta(0))
    stats['timeDiffShifted'] = stats['timeDiff'].shift().fillna(pd.to_timedelta(0))
    
    # print('Combined stats')
    # print(stats.head(20))
    # print(stats.tail(20))

    # Create an auxiliary dataframe df to work with receiver statistics, 
    # which can not be interpolated
    cols = [
        'pktRecv_rcv',
        'pktRcvLoss_rcv',
    ]
    cols_df = ['timeDiffShifted', 'timeDiff'] + cols

    # TODO: Optimize this
    df = stats[stats['isSender'] == False]
    df = df[cols_df]
    df = df.rename(columns={'timeDiffShifted': 'timeDiff_p1', 'timeDiff': 'timeDiff_p2'})

    df['timeDiff'] = df.index.to_series().diff()
    df['timeDiff'] = df['timeDiff'].fillna(df['timeDiff'].mean())
    
    df['timeDiff_p1'] = df['timeDiff_p1'] / df['timeDiff']
    df['timeDiff_p2'] = df['timeDiff_p2'] / df['timeDiff']

    for col in cols:
        df[f'{col}_p1'] = (df['timeDiff_p1'] * df[col]).round().astype('int32')
        df[f'{col}_p2'] = (df['timeDiff_p2'] * df[col]).round().astype('int32')
        df[f'{col}_adj'] = df[f'{col}_p1'] + df[f'{col}_p2'].shift().fillna(0)

    cols_adj = [f'{col}_adj' for col in cols]
    df = df[cols_adj]

    # print('Receiver df for calculations')
    # print(df.head(20))
    # print(df.tail(20))
    
    # In order to obtain the result dataframe, first join the auxiliary
    # dataframe df with adjusted receiver statistics to snd_stats and 
    # shift the data to correspond sender timepoints
    result = snd_stats.join(df, how='outer')
    result['isSender'] = result['isSender'].fillna(False)
    for col in cols_adj:
        result[col] = result[col].shift(-1)

    # Then, join the interpolated receiver statistics from stats dataframe
    cols_stats = ['msRTT_rcv' ,'mbpsBandwidth_rcv']
    result = result.join(stats[cols_stats], how='outer')

    # print('Intermediate result')
    # print(result.head(20))

    # Finally, extract sender observations only
    result = result[result['isSender']]

    cols_renamed = {
        'pktRecv_rcv_adj': 'pktRecv_rcv',
        'pktRcvLoss_rcv_adj': 'pktRcvLoss_rcv',
    }
    result = result.rename(columns=cols_renamed)

    cols_rearranged = [
        'pktSent_snd',
        'pktRecv_rcv',
        'pktSndLoss_snd',
        'pktRcvLoss_rcv',
        'msRTT_snd',
        'msRTT_rcv',
        'mbpsBandwidth_snd',
        'mbpsBandwidth_rcv'
    ]
    result = result[cols_rearranged]

    # print('Final result')
    # print(result.head(20))

    return result


def main():
    # Set filepaths to the source files: sender and receiver SRT core .csv statistics, tshark .pcapng dumps collected on both sides
    SND_STATS_PATH = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@23.96.93.54/4-srt-xtransmit-stats-snd.csv'
    RCV_STATS_PATH = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@40.69.89.21/3-srt-xtransmit-stats-rcv.csv'
    RCV_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@40.69.89.21/2-tshark-tracefile.pcapng'

    result = align_srt_stats(SND_STATS_PATH, RCV_STATS_PATH)


if __name__ == '__main__':
    main()