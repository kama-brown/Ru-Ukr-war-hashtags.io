# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 12:40:38 2022

@author: User
"""


def count_hashtags(data, n, groupby=None):
    import pandas as pd
    hashtag_count = {}
    data = data.dropna(subset=['Hashtags'])
    for idx, row in data.iterrows():
        if groupby is not None:
            if row[f'{groupby}'] not in hashtag_count:
                hashtag_count[row[f'{groupby}']] = {}
            for hashtag in row['Hashtags']:
                if hashtag not in hashtag_count[row[f'{groupby}']]:
                    hashtag_count[row[f'{groupby}']][hashtag] = 1
                else:
                    hashtag_count[row[f'{groupby}']][hashtag] += 1
        else:
            for hashtag in row['Hashtags']:
                if hashtag not in hashtag_count:
                    hashtag_count[hashtag] = 1
                else:
                    hashtag_count[hashtag] += 1

    hash_count = {}
    if groupby is not None:
        for k, v in hashtag_count.items():
            if k not in hash_count:
                hash_count[k] = {}
            for nk, nv in v.items():
                if nv >= n:
                    hash_count[k][nk] = nv
        hash_count_df = pd.DataFrame.from_dict({i: hash_count[i]
                                                for i in hash_count.keys()},
                                               orient='index')
        hash_count_df = hash_count_df.fillna(0)
    else:
        hash_count_df = pd.DataFrame.from_dict(hashtag_count,
                                               orient='index')
        hash_count_df = hash_count_df.reset_index()
        hash_count_df.columns = ['hashtag', 'count']
        hash_count_df = hash_count_df[hash_count_df['count'] >= n]
        hash_count_df = hash_count_df.fillna(0)

    return hash_count_df


def csv_to_sql(data, db, table, if_exists):

    import pandas as pd
    import sqlite3

    df = pd.read_csv(f'data/{data}.csv')
    df = df.iloc[:, 1:]
    df['Date'] = pd.to_datetime(df['Date'])

    con = sqlite3.connect(f"data/{db}.db")
    con.execute('PRAGMA foreign_keys = ON;')

    df.to_sql(f'{table}', con, if_exists=f'{if_exists}',
              dtype={
                  'Date': 'DATETIME',
                  'User': 'CHAR',
                  'Content': 'VARCHAR',
                  'Hashtags': ' VARCHAR',
                  'Likes': 'INT',
                  'Replies': 'INT',
                  'Retweets': 'INT'})

    print(con.execute('''
                SELECT * FROM sqlite_master;
                ''').fetchall())
    con.close()


def hashtags():
    hashtag = [
        '#ukraine', '#russia', '#russianukrainewar',
        '#ukrainewrussiawar', '#russianukrainianwar',
        '#ukrainewar', '#russiaukraineconflict',
        '#ukrainerussiaconflict', '#ukraineinvasion',
        '#russianinvasion', '#russiaukrainecrisis',
        '#ukrainecrisis', '#zelenskyy', '#putin', '#vladimirputin',
        '#volodymyrzelenskyy', '#russiaukraine', '#russianarmy',
        '#ukraineunderattack',
        '#istandwithputin', '#istandwithrussia',
        '#isupportrussia', '#naziukraine', '#standwithrussia',
        '#putinisright', '#standwithputin', '#supportrussia',
        '#zelenskywarcriminal',
        '#istandwithukraine', '#slavaukraine',
        '#slavaukraini', '#russiaisaterroriststate',
        '#nazirussia', '#putinwarcriminal',
        '#stopputinnow', '#putinwarcrimes',
        '#stopputin', '#stoprussia', '#istandwithzelenskyy',
        '#stoprussianagression', '#ukrainewillresist',
        '#saveukraine', '#ukrainewillwin', '#armukrainenow',
        '#russianarmy', '#helpukraine', '#genocideofukrainian',
        '#ukrainenews', '#wwiii', '#worldwar3', '#puckputin',
        '#putler', '#ukrainianarmy', '#ukraineunderattack',
        '#putinswar', '#ukrainian', '#nato', '#kyiv', '#kherson',
        '#mariupol', '#kharkiv', '#bakhmut', '#donbas',
        '#donbass', '#crimea', '#kiev', '#donetsk', '#odessa',
        '#bucha', '#luhansk', '#dnr', '#lpr', '#nafo',
        '#kremlin', '#moscow'
    ]
    return hashtag


def convert_typo():
    import pandas as pd
    con_typo_df = pd.DataFrame([
        [['зсу'], 'afu'],
        [['joebiden'], 'biden'],
        [['bitcoin'], 'btc'],
        [['中国'], 'china'],
        [['covid19', 'covid_19'], 'covid'],
        [['крым', 'krim', 'crimean', 'крим'], 'crimea'],
        [['donbass'], 'donbas'],
        [['donaldtrump'], 'trump'],
        [['europeanunion'], 'eu'],
        [['elonmusk'], 'musk'],
        [['fck'], 'fuck'],
        [['crimeanbridge', 'crimeabridge', 'кримськийміст'], 'kerchbridge'],
        [['kharkhiv', 'kharkov'], 'kharkiv'],
        [['marioupol', 'mauripol'], 'mariopol'],
        [['moskva'], 'moscow'],
        [['nafofellas'], 'nafo'],
        [['noflyzoneoverukraine', 'noflyzoneua'], 'noflyzone'],
        [['nfts'], 'nft'],
        [['odesa'], 'odessa'],
        [['prayforukraine', 'prayingforukraine'], 'pray_ukr'],
        [['ptn', 'vladimirputin'], 'putin'],
        [['putinhitler'], 'putler'],
        [['putiniswarcriminal', 'putinismasskiller',
          'putinisaterrorist', 'putinisawarcriminal',
          'putinwarcriminal'], 'putinwarcrimes'],
        [['republican'], 'gop'],
        [['俄罗斯', 'rusia', 'россия', 'ロシア', 'روسيا'], 'russia'],
        [['ucraina', 'ukrayna', 'ukrania', 'ucrania', 'ukraina',
          'ukriane', 'ukraine️', 'україна', '乌克兰', 'ουκρανία',
          'ουκρανια', 'украина', 'ウクライナ', 'أوكرانيا',
          'ukrainekrieg'], 'ukraine'],
        [['britain', 'unitedkingdom'], 'uk'],
        [['unitednations'], 'un'],
        [['unitedstates', 'america'], 'usa'],
        [['sovietunion'], 'ussr'],
        [['worldwar3', 'wwiii', 'worldwariii'], 'ww3'],
        [['xijinping'], 'xi'],
        [['zelenskyy', 'presidentzelensky', 'selenskyj',
          'volodymyrzelensky', 'zelenskiy', 'zelenskyua'], 'zelensky'],
        [['slavaukrainii', 'slavaukraïni', 'slavaukraine', 'glorytoukraine',
          'славаукраїні', 'slavaukrayini'],
         'slavaukraini'],
        [['stopputinnow', 'stopputin', 'stopputinswar',
          'stoprussianow', 'stoprussia', 'stoprussianaggression',
          'stopurssianwar'], 'stop_ru_war'],
        [['ukrainerussianwar', 'russiaukrainewar',
          'ukrainerussiawar', 'russianukrainianwar', 'russianukrainewar',
          'russiaukrainwar', 'ukrainewar', 'war_in_ukraine', 'вторжениероссии',
          'warinukraine', 'warukraine'], 'ukraine_war'],
        [['russiaukraineconflict', 'ukrainerussiaconflict',
         'ukraineconflict'], 'ukraine_conflict', ],
        [['russiaukrainecrisis', 'ukrainecrisis',
          'ukrainerussiacrisis'], 'ukraine_crisis'],
        [['standforukraine', 'standupforukraine', 'standingwithukraine',
          'standwithukraine', 'staywithukraine', 'unitedwithukraine',
          'westandwithukraine', 'istandforukraine', 'isupportukraine'], 'support_ukr'],
        [['istandwithrussia', 'standwithrussia',
          'istandwithputin', 'standwithputin'], 'support_ru'],
        [['ukraineinvasion', 'russianinvasion', 'russiainvadedukraine',
          'russiainvadeukraine', 'russiainvasion', 'russiainvadesukraine'],
         'ukraine_invasion'],
        [['russiaisaterroriststate', 'russiaisaterrorisstate',
         'russiaterroriststte', 'russiaisateroriststate',
          'russiaisaterroristate', 'russiaisaterroiststate',
          'russiaisanazistate'], 'ru_terroriststate'],
        [['russiancrime', 'russianwarcrime', 'russianwarcrimesinukraine',
          'urssiawarcrime'], 'ru_warcrime'],
        [['ukrainewillprevail', 'ukrainewillresist',
          'ukrainswillresist'], 'ukrainewillwin']])

    con_typo_df.columns = ['original', 'result']
    return con_typo_df


def get_lemma(word):
    import nltk
    nltk.download('wordnet', quiet=True)
    from nltk.corpus import wordnet as wn
    lemma = wn.morphy(word)
    if lemma is None:
        return word
    else:
        return lemma


def df_to_wed(df, period=None, portion=500, threshold=200):

    import itertools
    import pandas as pd
    col1 = []
    col2 = []
    if period is not None:
        groupls = []
    for index, row in df.iterrows():
        for n in list(itertools.combinations(row['Hashtags'], 2)):
            col1.append(n[0])
            col2.append(n[1])
            if period is not None:
                groupls.append(row[f'{period}'])
    if period is None:
        edges = list(zip(col1, col2))
        ed_df = pd.DataFrame(edges, columns=['source', 'target'])
        edges = [sorted(edge) for edge in edges]
        wed_df = pd.DataFrame(
            {'weight': ed_df.groupby(['source', 'target']).size()}
        ).reset_index()
    else:
        edges = list(zip(groupls, col1, col2))
        ed_df = pd.DataFrame(edges, columns=[f'{period}', 'source', 'target'])
        wed_df = pd.DataFrame(
            {'weight': ed_df.groupby(
                [f'{period}', 'source', 'target']).size()}
        ).reset_index()

    wed_1 = wed_df[wed_df['weight'] >= (max(wed_df['weight'])/portion)]

    if period is None:
        return wed_1

    wed = pd.DataFrame()

    if period == 'Week':

        for i in range(max(wed_1['Week'])):
            wed_w_1 = wed_1[wed_1['Week'] == i].sort_values(
                ['weight'], ascending=False)[:threshold]
            wed = pd.concat([wed, wed_w_1])

    if period == 'Date':
        from dateutil.rrule import rrule, DAILY
        startdate = min(wed_1['Date'])
        enddate = max(wed_1['Date'])
        for dt in rrule(DAILY, dtstart=startdate, until=enddate):
            wed_d_1 = wed_1[wed_1['Date'] == dt.date()].sort_values(
                ['weight'], ascending=False)[:threshold]
            wed = pd.concat([wed, wed_d_1])

    return wed


def wed_to_nodetable(wed, period=None):

    import pandas as pd
    node_dict = {}

    wed['type'] = 'Undirected'
    if period == None:
        wed = wed[['source', 'target', 'type', 'weight']]

        for idx, row in wed.iterrows():
            if row['source'] not in node_dict:
                node_dict[row['source']] = row['weight']
            else:
                node_dict[row['source']] += row['weight']
            if row['target'] not in node_dict:
                node_dict[row['target']] = row['weight']
            else:
                node_dict[row['target']] += row['weight']

        node_df = pd.DataFrame.from_dict(node_dict, orient='index')
        node_df = node_df.reset_index()
        node_df.columns = ['id', 'weight']
        return node_df, wed

    else:
        wed = wed[['source', 'target', 'type', 'weight', f'{period}']]

        for idx, row in wed.iterrows():
            if row['source'] not in node_dict:
                node_dict[row['source']] = {'timestamp': [row[f'{period}']],
                                            'score': {
                    row[f'{period}']: row['weight']}}
            else:
                if row[f'{period}'] not in node_dict[
                        row['source']]['timestamp']:
                    node_dict[row['source']]['timestamp'].append(
                        row[f'{period}'])
                    node_dict[row['source']]['score'][row[f'{period}']
                                                      ] = row['weight']
                else:
                    node_dict[row['source']]['score'][row[f'{period}']
                                                      ] += row['weight']

            if row['target'] not in node_dict:
                node_dict[row['target']] = {'timestamp': [row[f'{period}']],
                                            'score': {
                    row[f'{period}']: row['weight']}}
            else:
                if row[f'{period}'] not in node_dict[
                        row['target']]['timestamp']:
                    node_dict[row['source']]['timestamp'].append(
                        row[f'{period}'])
                    node_dict[row['target']]['score'][row[f'{period}']
                                                      ] = row['weight']
                else:
                    node_dict[row['target']]['score'][row[f'{period}']
                                                      ] += row['weight']

        for k, v in node_dict.items():
            v['score'] = [(kk, vv) for kk, vv in v['score'].items()]
        node_df = pd.DataFrame.from_dict(node_dict, orient='index')
        return node_df, wed


def wed_to_nodesize(wed, period):
    node_size = {}
    for idx, row in wed.iterrows():
        if row[period] not in node_size:
            node_size[row[period]] = {row['source']: row['weight']}
            node_size[row[period]][row['target']] = row['weight']
        if row['source'] not in node_size[row[period]]:
            node_size[row[period]][row['source']] = row['weight']
        else:
            node_size[row[period]][row['source']] += row['weight']
        if row['target'] not in node_size[row[period]]:
            node_size[row[period]][row['target']] = row['weight']
        else:
            node_size[row[period]][row['target']] += row['weight']
    return node_size
