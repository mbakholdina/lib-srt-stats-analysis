""" TODO """
import pathlib

# import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
# import seaborn as sns
import streamlit as st

from tcpdump_processing import convert, extract_packets


def print_problem_places(stats):
    # TODO: Delete this later
    # Problem places for debugging purposes
    print('\nProblem places')
    problem_1 = stats['2020-10-02 17:34:30.275586':'2020-10-02 17:34:30.307844']
    print('\n Problem 1')
    print(problem_1)

    problem_2 = stats['2020-10-02 17:34:30.510054': '2020-10-02 17:34:30.540437']
    print('\n Problem 2')
    print(problem_2)


def fix_stats_dataframe(stats):
    # If stats['isSenderCheck'] == True, it means we've found the place where
    # there are two or more consecutive points for receiver/sender.
    # The first value is always False.

    print_problem_places(stats)

    # TODO: Delete this later
    print('\nIntroducing several consecutive problem places')
    stats.loc['2020-10-02 17:34:30.285718'] = [
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        False,
        69.578,
        85.284,
        97.0,
        0.0
    ]

    print_problem_places(stats)

    #### Function

    cols_aggreg_snd = [
        'pktSent_snd',
        'pktSndLoss_snd',
    ]
    cols_aggreg_rcv = [
        'pktRecv_rcv',
        'pktRcvLoss_rcv',
    ]
    cols_to_fix = cols_aggreg_rcv + cols_aggreg_snd

    i = 0

    while(True):
        print(f'\nIteration: {i}')

        stats['toFix'] = ~stats['isSender'].diff().fillna(True)

        print(f"\nPoints to fix: {stats['toFix'].sum()}")

        print_problem_places(stats)

        if stats['toFix'].sum() == 0:
            print('Exiting from while cycle')
            # break - additional checks
            return

        # TODO: Check boundaries for shift, plus do not forget about assumption
        # that stats dataframe should have first and last row from sender - make an assertion
        # Fix rows
        for col in cols_to_fix:
            stats.loc[(stats['toFix'] == True) & (stats['toFix'].shift() == False), col] = stats[col] + stats[col].shift()
            
        print('\nFixed rows')
        print(stats.info())
        print_problem_places(stats)

        # Drop rows on top of fixed rows
        indexs_to_drop = stats[(stats['toFix'] == False) & (stats['toFix'].shift(-1) == True)].index
        stats.drop(indexs_to_drop, inplace=True)

        # TODO: Check that we can not drop first and last rows
        print('\nDropped rows')
        print(stats.info())
        print_problem_places(stats)

        i += 1
    
    # TODO: Additional checks + throw exception
    # TODO: Add check that there is no situations left - that column check = False
    # TODO: Check that the number of sender point is higher by 1 than the number of rcv points


def align_srt_stats_v1(snd_stats_path: str, rcv_stats_path: str):
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

    print('\nSender stats')
    print(snd_stats.head(10))
    print(snd_stats.tail(10))
    print('\nReceiver stats')
    print(rcv_stats.head(10))
    print(rcv_stats.tail(10))

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
    
    print('\nJoined stats')
    print(stats.head(10))
    print(stats.tail(10))

    # The algorithm we are going to apply for aggregated statistics
    # has one assumption: in joined dataframe there should be no consecutive
    # datapoints from receiver or sender. All measurements should correspond
    # to: one point from sender, one point from receiver, one point from sender,
    # etc. See column stats['isSender'] which is True in case sender data
    # point and False in case receiver data point.
    fix_stats_dataframe(stats)

    # Do linear interpolation for features where applicable
    stats['msRTT_rcv'] = stats['msRTT_rcv'].interpolate()
    stats['msRTT_rcv'] = stats['msRTT_rcv'].fillna(method='bfill')

    stats['mbpsBandwidth_rcv'] = stats['mbpsBandwidth_rcv'].interpolate()
    stats['mbpsBandwidth_rcv'] = stats['mbpsBandwidth_rcv'].fillna(method='bfill')

    print('\nInterpolated stats')
    print(stats.head(10))
    print(stats.tail(10))
    print(stats.info())
    print(stats.describe())

    # return
    
    # The rest statistics is the aggredated statistics, so we will apply
    # special technique to align the frames
    stats['timeDiff'] = stats.index.to_series().diff().fillna(pd.to_timedelta(0))
    stats['timeDiffShifted'] = stats['timeDiff'].shift().fillna(pd.to_timedelta(0))
    
    print('TimeDiff')
    print(stats.head())
    print(stats.tail())

    # Create an auxiliary dataframe df to work with receiver statistics, 
    # which can not be interpolated
    cols = [
        'pktRecv_rcv',
        'pktRcvLoss_rcv',
    ]
    cols_df = ['timeDiffShifted', 'timeDiff'] + cols

    df = stats[stats['isSender'] == False]
    df = df[cols_df]
    df = df.rename(columns={'timeDiffShifted': 'timeDiff_p1', 'timeDiff': 'timeDiff_p2'})

    print(df.head())

    df['timeDiff'] = df.index.to_series().diff()
    df['timeDiff'] = df['timeDiff'].fillna(df['timeDiff'].mean())
    
    df['timeDiff_p1'] = df['timeDiff_p1'] / df['timeDiff']
    df['timeDiff_p2'] = df['timeDiff_p2'] / df['timeDiff']

    print(df.head())

    for col in cols:
        df[f'{col}_p1'] = (df['timeDiff_p1'] * df[col]).round().astype('int32')
        df[f'{col}_p2'] = (df['timeDiff_p2'] * df[col]).round().astype('int32')
        df[f'{col}_adj'] = df[f'{col}_p1'] + df[f'{col}_p2'].shift().fillna(0)

    print(df.head(20))

    cols_adj = [f'{col}_adj' for col in cols]
    df = df[cols_adj]

    print('Receiver df for calculations')
    print(df.head(20))
    print(df.tail(20))
    
    # TODO: Check for NaNs
    
    # In order to obtain the result dataframe, first extract sender statistics
    # and interpolated rcv statistics from stats dataframe, second join the auxiliary
    # dataframe df with adjusted aggregated receiver statistics, then
    # shift df columns data one point up to correspond sender timepoints,
    # and finally extract sender observations only
    cols_stats = [
        'isSender',
        'pktSent_snd',
        'pktSndLoss_snd',
        'msRTT_snd',
        'mbpsBandwidth_snd',
        'msRTT_rcv',
        'mbpsBandwidth_rcv'
    ]
    result = stats[cols_stats]
    result = result.join(df, how='outer')

    print('Intermediate result - before shift')
    print(result.head(10))
    print(result.tail(10))

    for col in cols_adj:
        result[col] = result[col].shift(-1)
        # TODO: The last observation here is Nan -> fill

    print('Intermediate result - after shift')
    print(result.head(10))
    print(result.tail(10))

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

    print('Final result')
    print(result.head(10))
    print(result.tail(10))
    print(result.info())

    return result


def align_srt_stats_v2(snd_stats_path: str, rcv_stats_path: str):
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

    print('\nSender stats')
    print(snd_stats.head(10))
    print(snd_stats.tail(10))
    print('\nReceiver stats')
    print(rcv_stats.head(10))
    print(rcv_stats.tail(10))

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
    # TODO: I've tested caller-snd and listener-rcv setup.
    # Check additionally how this behaves in case of caller-rcv and 
    # listener-snd setup
    start_timestamp = max(snd_stats.index[0], rcv_stats.index[0])
    end_timestamp = min(snd_stats.index[-1], rcv_stats.index[-1])
    stats = stats[(stats.index >= start_timestamp) & (stats.index <= end_timestamp)]

    # Second, we check that the first and the last timepoints are both
    # sender timepoints. If not, drop them.
    if not stats['isSender'][0]:
        stats = stats[1:]

    if not stats['isSender'][-1]:
        stats = stats[:-1]
    
    print('\nJoined stats')
    print(stats.head(10))
    print(stats.tail(10))

    # Do linear interpolation for features where applicable
    cols_to_round = [
        'pktSent_snd',
        'pktSndLoss_snd',
        'pktRecv_rcv',
        'pktRcvLoss_rcv',
    ]
    stats.loc[:, stats.columns != 'isSender'] = stats.interpolate().fillna(method='bfill')
    stats.loc[:, cols_to_round] = stats.round()

    print('\nInterpolated stats')
    print(stats.head(10))
    print(stats.tail(10))

    stats = stats[stats['isSender']]

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
    stats = stats[cols_rearranged]

    print('Final result')
    print(stats.head(10))
    print(stats.tail(10))
    print(stats.info())

    return stats


def plot_scatter(
    title,
    x_metric,
    y_metrics,
    x_axis_title,
    y_axis_title,
    df_aggregated
):
    """
    Function to plot and format a scatterplot from the aggregated dataframe
    """
    data = []
    shapes = list()
    for y_metric in y_metrics:
        data.append(
            go.Scatter(
                x=df_aggregated.index,
                y=df_aggregated[y_metric],
                mode='lines',
                marker=dict(opacity=0.8, line=dict(width=0)),
                name=y_metric
            )
        )

    fig = go.Figure(data=data)
    fig.update_layout(
        title=title,
        xaxis_title=x_axis_title,
        yaxis_title=y_axis_title,
        legend=go.layout.Legend(
            x=0,
            y=1,
            traceorder="normal",
            font=dict(family="sans-serif", size=8, color="black"),
            bgcolor="LightSteelBlue",
            bordercolor="Black",
            borderwidth=2
        ),
        shapes=shapes
    )
    st.plotly_chart(fig)


def main():
    # Set filepaths to the source files: sender and receiver SRT core .csv statistics, tshark .pcapng dumps collected on both sides
    # SND_STATS_PATH = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@23.96.93.54/4-srt-xtransmit-stats-snd.csv'
    # RCV_STATS_PATH = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@40.69.89.21/3-srt-xtransmit-stats-rcv.csv'
    # RCV_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@40.69.89.21/2-tshark-tracefile.pcapng'

    SND_STATS_PATH = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@23.96.93.54/4-srt-xtransmit-stats-snd.csv'
    RCV_STATS_PATH = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@40.69.89.21/3-srt-xtransmit-stats-rcv.csv'
    RCV_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@40.69.89.21/2-tshark-tracefile.pcapng'

    result = align_srt_stats_v2(SND_STATS_PATH, RCV_STATS_PATH)

    st.title('Title')

    st.subheader('Joined stats')
    st.write(result)
    st.write(result.describe())
    st.write(result.info())
    plot_scatter(
        'Synchronized stats',
        'Time',
        result.columns,
        'Time',
        'SYNCHRONIZED',
        result
    )


if __name__ == '__main__':
    main()