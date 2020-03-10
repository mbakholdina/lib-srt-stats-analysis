# Notes

## Aligning datasets

### Aligning SRT sender and receiver statistics

1. The timeline of the sender statistics is used to generate the result dataframe timeline. First, the intersection between sender and receiver timelines is found so that the first and the last timepoints correspond to sender timepoints. Second, an interpolation of receiver data is done to fill in missing values that correspond to sender datapoints. Finally, after some manipulations with data, only sender datapoints are extracted to form the timeline of the result aligned dataframe.

2. It is important to note that currently before aligning sender and receiver statisitcs, there is no shift of receiver timeseries (by RTT/2) done. 

    Receiver statistics is statistics from the past. Ideally, before joining sender and receiver datasets, we should shift receiver stats up by RTT/2. However, there possible difficulties here: 1) During the transmission RTT varies; 2) The accuracy of RTT estimation.
    
    There are two possible approaches:
    * Calculate initial RTT from handshakes exchange and shift datasets by RTT_initial/2 under assumption that the experiment time is small enough to have no changes in RTT during transmission;
    * Extract RTT estimation from tshark data (UMSG_ACK packets) or use `msRTT` SRT statisitcs and shift receiver datasets accordingly. This requires additional research. Problems: a) Estimation accuracy; b) What will hapen in time moments where RTT changes dramatically?

See `_data/notes_useast_eunorth_10.02.20_100Mbps.pdf` for the illustration.

### Aligning SRT statistics and tshark data

SRT statistics here means aligned SRT sender and receiver statistics.

1. The timeline of the SRT statistics is used to generate the result dataframe timeline. First, tshark dump is processed to extract SRT UMSG_ACK control packets only that contain valuable for the further analysis information (e.g., rtt, bandwidth, and receiving speed estimations). Second, and intersection between SRT stats and tshark data is found so that the first and the last timepoints correspond to SRT stats timepoints. Next, an interpolation of tshark data is done to fill in missing values that correspond to SRT stats datapoints. Finally, after some manipulations with data, only SRT stats datapoints are extracted to form the timeline of the result aligned dataframe.

2. It is important to note that tshark data collected at the receiver (not sender) side is used when aligning datasets. It is done in order to get as closer (real) timestamp nearby tshark data as possible. The SRT receiver calculates estimations (e.g., rtt, bandwidth, and receiving speed estimations) right before sending an acknowledgement UMSG_ACK packet back to the SRT sender. This packet will be registered by tshark at the reciver side and contain an absolute timestamp of sending the packet. The timestamp of the same packet registered at the sender time will be shifted by RTT/2 plus there is allways an opportunity to loss the packet during transmission.