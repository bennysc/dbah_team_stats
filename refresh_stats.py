
import requests
import pandas as pd
import boto3
import os
import logging

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

spaces_key = os.environ.get('SPACES_KEY')
spaces_secret = os.environ.get('SPACES_SECRET')


headers = {
    'authority': 'na.op.gg',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
    'accept': 'application/json, text/plain, */*',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://na.op.gg/summoners/na/snakebae',
    'accept-language': 'en-US,en;q=0.9',
    # Requests sorts cookies= alphabetically
    # 'cookie': '_fbp=fb.1.1644433738919.346541530; Hm_lvt_29884b6641f1b5709cc89a8ce5a99366=1644433740; _pbjs_userid_consent_data=3524755945110770; _lr_env_src_ats=false; pbjs-unifiedid=%7B%22TDID%22%3A%22c7538236-5b32-4f80-8a62-41ac9fb8bfc2%22%2C%22TDID_LOOKUP%22%3A%22TRUE%22%2C%22TDID_CREATED_AT%22%3A%222022-01-09T19%3A09%3A17%22%7D; __gads=ID=4abb6bf082780f5e:T=1644871947:S=ALNI_MZi93-agMOUZ5qEkgcPaiT0GT8H3Q; sharedid=66561e11-910c-43ed-96ef-0ad79bbe4524; CRISPSUBNO=; _gid=GA1.2.13836603.1647993080; _lr_geo_location=US; _lr_retry_request=true; _ga=GA1.2.1829847438.1644433739; cto_bundle=mnvEdV84JTJGTDdUR2E0WEhWUzZOOU5aUEs1JTJGQjVUd3Rub2xzYyUyQmM3T3hKMkRNTERKY1AlMkJJJTJGOVJzb1FaRGExM01zc3dEalZTRUVtVW9PV3FkT2NjTkxvTjAwa0xVb1NSNjV4VSUyRlp5Yk9QUDVxWDEwblNGaWxzOFglMkZhRTBRS3BtV3MyWSUyQmxVNUpNVXVJSXVxQlIyem5ZQnRybmFRJTNEJTNE; _ga_HKZFKE5JEL=GS1.1.1648071273.56.1.1648072173.0; _ga_37HQ1LKWBE=GS1.1.1648071273.55.1.1648072173.0; _ga_FDHPWC794N=GS1.1.1648071273.56.1.1648072173.0; _dd_s=rum=0&expire=1648073077103',
}





def get_summoner_info(name):
    print(f'Getting summoner info for {name}')
    url = f'https://na.op.gg/api/summoners/na/autocomplete?keyword={name}'
    r = requests.get(url, headers=headers)
    results = r.json()
    for summoner in results['data']:
        if summoner['internal_name'] == name:
            return summoner
    return None


def get_summoner_games(summoner_id):
    params = {
        'hl': 'en_US',
        'game_type': 'TOTAL',
    }
    r = requests.get(
        f'https://na.op.gg/api/games/na/summoners/{summoner_id}', headers=headers, params=params)
    games = r.json()
    return games['data']


def get_champions():
    print('Getting champions...')
    r = requests.get(
        'https://na.op.gg/api/meta/champions?hl=en_US', headers=headers)
    champions = r.json()
    return champions['data']


dbah_team = ['cllassy66', 'joeybagadonitz', 'raythar', 'masterbait69', 'cantonesports', 'ewxipqy', 'codybarrese', 'jewflayer',
             'snakebae', 'dbahdaspooper', 'callmédad', 'winters', 'vulcan101', 'calvinious', 'shebbles', 'braveclue', 'snivel100', 'beastops331', '100moe']

def num_dbah_players(names):
    names_list = [x.lower() for x in list(names)]
    count = 0
    for name in names_list:
        if name in dbah_team:
            count += 1
    return count


def is_dbah_team(names):
    names_list = [x.lower() for x in list(names)]
    for name in names_list:
        if name in dbah_team:
            return True
    return False


def parse_games(games):
    df = pd.DataFrame(games).explode(['participants']).reset_index(drop=True)

    df = df.join(pd.json_normalize(df['participants']))
    champions = get_champions()
    champion_df = pd.DataFrame(champions)
    champion_df.columns = ['champion_'+str(x) for x in champion_df.columns]

    df = df.merge(champion_df, left_on=['champion_id'], right_on=[
                  'champion_id'], how='left')
    df['team_data'] = df.apply(
        lambda l: [x for x in l['teams'] if x['key'] == l['team_key']][0], axis=1)
    df = df.join(pd.json_normalize(df['team_data']))

    dfg = df.groupby(['id'], as_index=False).agg({'summoner.internal_name': num_dbah_players}).rename(
        columns={'summoner.internal_name': 'num_dbah_players'})
    dfg2 = df.groupby(['id', 'team_key'], as_index=False).agg(
        {'summoner.internal_name': is_dbah_team}).rename(columns={'summoner.internal_name': 'is_dbah_team'})

    df = df.merge(dfg, left_on=['id'], right_on=['id'], how='left')
    df = df.merge(dfg2, left_on=['id', 'team_key'],
                  right_on=['id', 'team_key'], how='left')
    return df


def refresh_stats():
    champions = get_champions()
    recs = []
    for name in dbah_team:
        s = get_summoner_info(name)
        games = get_summoner_games(s['summoner_id'])
        
        if len(games) > 0:
            num_games = len(games)
            print(f'{num_games} games retrieved for {name}')
            tdf = parse_games(games)
            recs.append(tdf)

    df = pd.concat(recs)
    df = df.groupby(['id', 'summoner.internal_name'], as_index=False).first(
    ).sort_values(['created_at', 'id', 'team_key'])
    df['game_length_minute'] = df['game_length_second'] / 60
    select_df = df[(df['num_dbah_players'] == 5) & (df['is_dbah_team'])][['id', 'created_at', 'summoner.internal_name', 'position', 'stats.kill', 'stats.death', 'stats.assist', 'stats.total_damage_dealt_to_champions',
                                                                          'stats.total_damage_taken', 'stats.ward_place', 'stats.minion_kill', 'stats.gold_earned', 'game_length_minute', 'champion_name', 'stats.op_score', 'stats.result', 'num_dbah_players', 'is_dbah_team']]
    select_df = select_df.rename(columns={'created_at': 'date', 'summoner.internal_name': 'name', 'position': 'role', 'stats.kill': 'kills', 'stats.death': 'deaths', 'stats.assist': 'assists', 'stats.total_damage_dealt_to_champions': 'damage_dealt',
                                 'stats.total_damage_taken': 'damage_taken', 'stats.ward_place': 'wards', 'stats.minion_kill': 'cs', 'stats.gold_earned': 'gold', 'game_length_minute': 'minutes', 'champion_name': 'champion', 'stats.op_score': 'op_score', 'stats.result': 'result'})

    select_df['unique_id'] = (select_df['date'] + '_' + (select_df['minutes'] - (select_df['minutes'] % 1)).astype(int).astype(str) + '_' + ((select_df['minutes'] % 1) * 60).astype(int).astype(str) + '_' + select_df['result'] + '_' + select_df['name']).str.lower()
    num_team_games = len(select_df)

    print(f'{num_team_games} additional games retrieved')
    print('Fetching existing data...')
    existing_data = pd.read_csv('https://league-stats.nyc3.digitaloceanspaces.com/dbah_stats.csv')
    num_existing_games = len(existing_data)
    print(f'{num_existing_games} existing games retrieved')
    unique = pd.concat([existing_data,select_df]).groupby(['id','name'],as_index=False).first().sort_values(['date','name'])
    num_unique_games = len(unique)
    print(f'{num_unique_games} total unique games')

    print('Updating data')
    session = boto3.session.Session()
    client = session.client('s3',
                region_name='nyc3',
                endpoint_url='https://nyc3.digitaloceanspaces.com',
                aws_access_key_id=spaces_key,
                aws_secret_access_key=spaces_secret)

    client.put_object(Bucket='league-stats',
                  Key='dbah_stats.csv',
                  Body=unique.to_csv(index=False),
                  ACL='public-read'
                )