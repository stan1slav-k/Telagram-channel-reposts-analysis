from pyvis.network import Network
import asyncio
import pandas as pd

from telethon.sync import TelegramClient

# Class for working with messages
from telethon.tl.functions.messages import GetHistoryRequest

# Set settings for connection
api_id = 'Your api id'
api_hash = 'Your api hash'
username = 'Your username'

proxy_server = '149.154.167.50'
proxy_port = '443'
proxy_key = 'Your proxy key'

proxy = (proxy_server, proxy_port, proxy_key)
client = TelegramClient(username, api_id, api_hash)
client.start()


async def dump_all_messages(channels, order, depth_limit_counter=0, already_parsed=[]):

    depth_limit = 700  # how deep we will parse channels
    df = pd.DataFrame()
    all_messages = []  # list for parsed messages
    channel = ''

    offset_msg = 0  # number for start message
    limit_msg = 1000  # max amount of parsed messages for channel

    """ Some channels are private and you can't get 
    data from them, we have to handle this situation """
    while order <= len(channels):
        try:
            ch_id = channels.iloc[order]['fwd_from_id']
            if ch_id not in already_parsed:
                already_parsed.append(ch_id)
                channel = await client.get_entity(int(ch_id))
                break
            else:
                order += 1
        except:
            order += 1
            if order == len(channels):
                depth_limit_counter = depth_limit + 1
                break

    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_msg,
            offset_date=None, add_offset=0,
            limit=limit_msg, max_id=0, min_id=0,
            hash=0))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            all_messages.append(message.to_dict())

            try:
                fwrd_channel_id = message.fwd_from.from_id.channel_id
            except:
                fwrd_channel_id = ''

            new_row = pd.Series({'channel_id': message.peer_id.channel_id if message.peer_id else '',
                                 'fwd_from_id': fwrd_channel_id})
            df = pd.concat([df, new_row.to_frame().T], ignore_index=True)

        """ Group forwarded messages and count amount of reposts """
        unique_fwd_channels = df.groupby('fwd_from_id') \
                                .count() \
                                .sort_values('channel_id', ascending=False)['channel_id'] \
                                .reset_index() \
                                .rename(columns={'channel_id': 'num_of_reposts'})

        unique_fwd_channels['fwd_to_id'] = channel.id
        unique_fwd_channels['fwd_to_title'] = channel.title
        unique_fwd_channels['fwd_to_username'] = "@" + str(channel.username)

        channels = pd.concat([channels, unique_fwd_channels.iloc[1:]])
        depth_limit_counter += 1
        order += 1

        # Continue parsing until we reach searching depth
        if depth_limit_counter > depth_limit:
            return channels
        else:
            return await dump_all_messages(channels, order, depth_limit_counter, already_parsed)


async def main():
    # Link to first channel, from which we start parsing
    url = 'https://t.me/PeeAcE_DaTa'

    selected_channel = await client.get_entity(url)
    channel_list = pd.DataFrame({'fwd_to_id': [0],
                                 'fwd_to_title': [''],
                                 'fwd_to_username': [''],
                                 'num_of_reposts': [0],
                                 'fwd_from_id': [selected_channel.id]})

    # Recursive func to get data about forward between channels
    channel_list = await dump_all_messages(channel_list, 0)
    channel_list = channel_list.iloc[1:]

    """ When we grab data we receive only forwarded from id, and we need to enrich the data
    with channel title and name. Do it inside function makes a lot of unnecessary calls,
    so i decide to do it separately. Additional problem that get channel info task asynchronous,
    and we have to gather all task together"""
    tasks = []
    for elem in channel_list.fwd_from_id.unique().tolist():
        tasks.append(get_channel_info(elem))

    fwd_from_channels = await asyncio.gather(*tasks)
    fwd_from_channels = pd.DataFrame(fwd_from_channels).dropna()
    fwd_from_channels = fwd_from_channels.astype({'fwd_from_id': 'int'})
    channel_list = channel_list.merge(fwd_from_channels, how='left', on='fwd_from_id')

    # Create graph and save it in
    create_graph(channel_list)

    # Save results in csv
    channel_list.to_csv('channel_list.csv', index=False)


async def get_channel_info(fwd_from_id):
    try:
        channel_info = await client.get_entity(fwd_from_id)
        new_row = pd.Series({'fwd_from_id': channel_info.id,
                             'fwd_from_title': channel_info.title,
                             'fwd_from_username': '@' + channel_info.username})
        return new_row
    except:
        return pd.Series({'fwd_from_id': None, 'fwd_from_title': None, 'fwd_from_username': None})


def create_graph(channel_list):
    """Prepare data ad creat graph for channes connecitions"""
    df = channel_list.copy()
    # You may clear this query, but in this case graph became unreadable because of o lot of data
    df = df.query('num_of_reposts > 2')
    df = df.dropna()
    df = df.astype({'fwd_from_id': 'int', 'fwd_to_id': 'int', 'num_of_reposts': 'int'})

    # count for each channel how much reports they made (from other channels)
    total_repost_to_df = df.groupby('fwd_to_id') \
                           .agg({'num_of_reposts': 'sum'}).copy() \
                           .rename(columns={'num_of_reposts': 'total_reposts_to'}).reset_index()
    df = df.merge(total_repost_to_df, how='left', on='fwd_to_id')

    # count for each channel how much reports was made from this channel to another
    total_repost_from_df = df.groupby('fwd_from_id') \
                             .agg({'num_of_reposts': 'sum'}).copy() \
                             .rename(columns={'num_of_reposts': 'total_reposts_from'}).reset_index()
    df = df.merge(total_repost_from_df, how='left', on='fwd_from_id')

    # Set max node size as 50. for that purpose lets calculate coefficient for adjustments all nodes
    node_size_coeff = 50 / df.total_reposts_from.max()

    net = Network()
    # For each row create two nodes and edge between them
    for row in df.itertuples():
        net.add_node(row.fwd_from_username, row.fwd_from_username, title=str(row.fwd_from_title),
                     size=max(int(row.total_reposts_from * node_size_coeff), 15))
        net.add_node(row.fwd_to_username, row.fwd_to_username, title=str(row.fwd_to_title),
                     size=max(int(row.total_reposts_from * node_size_coeff), 15))
        net.add_edge(row.fwd_from_username, row.fwd_to_username, value=row.num_of_reposts)

    # Show additional settings below graph
    net.show_buttons(filter_=['nodes', 'edges', 'physics'])
    # Save result in channel_list_graph.html
    net.show("channel_list_graph.html")

    # additionally lets get list of top reposted/reposter channels
    most_repostable_channels = df[['total_reposts_from', 'fwd_from_title']] \
        .groupby(['total_reposts_from', 'fwd_from_title'], as_index=False).count() \
        .sort_values('total_reposts_from', ascending=False) \
        .reset_index().drop(columns='index').head(10)

    most_reposts_includes_channels = df[['total_reposts_to', 'fwd_to_title']] \
        .groupby(['total_reposts_to', 'fwd_to_title'], as_index=False).count() \
        .sort_values('total_reposts_to', ascending=False) \
        .reset_index().drop(columns='index').head(10)

    print(f"Top repostable channels: \n {most_repostable_channels}\n\n\
    Top channel with repost incide: \n {most_reposts_includes_channels}")


with client:
    client.loop.run_until_complete(main())
