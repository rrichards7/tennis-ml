import csv
import re
import requests
import os
import sys
import argparse
import time
import datetime
import pandas as pd
import requests
import concurrent.futures
import json
import pprint
import pycurl
from lxml import html, etree
from io import BytesIO 

def array2csv(array, filename):
    print("in writing array2csv")
    with open(filename, "w+") as my_csv:
        csvWriter = csv.writer(my_csv, delimiter = ',')
        csvWriter.writerows(array)

def curl_html_parse_tree(url, headers=None):
    b_obj = BytesIO() 
    crl = pycurl.Curl() 

    # Set URL value
    crl.setopt(crl.URL, url)
    if headers is not None:
        crl.setopt(pycurl.HTTPHEADER, headers)
    # Write bytes that are utf-8 encoded
    crl.setopt(crl.WRITEDATA, b_obj)

    # Perform a file transfer 
    crl.perform() 

    # End curl session
    crl.close()

    # Get the content stored in the BytesIO object (in byte characters) 
    get_body = b_obj.getvalue()

    # Decode the bytes stored in get_body to HTML and print the result 
    #print('Output of GET request:\n%s' % get_body.decode('utf8')) 
    return html.fromstring(get_body.decode('utf8'))

def html_parse_tree(url, headers=None):
    page = requests.get(url, headers=headers)

    if headers is not None:
        pprint.pprint(page.request.headers)
        print(page.content)
    tree = html.fromstring(page.content)
    return tree

def xpath_parse(tree, xpath):
    result = tree.xpath(xpath)
    return result

def xpath_parse_first(tree, xpath):
    result = tree.xpath(xpath)
    if len(result) > 0:
        return result[0]
    return None

def regex_strip_array(array):
    for i in range(0, len(array)):
        array[i] = regex_strip_string(array[i]).strip()
    return array

def regex_strip_string(string):
    string = re.sub('\n', '', string).strip()
    string = re.sub('\r', '', string).strip()
    string = re.sub('\t', '', string).strip()
    return string

def get_tournaments(division):
    print(f"Getting Touranments for {division}...")
    # Setup
    url_prefix = "https://www.tennis24.com/"
    tournament_tree = html_parse_tree(url_prefix)
    #print(etree.tostring(tournament_tree))
    #womens id
    singles_division_id = "lmenu_5725"
    if division == 'ATP':
        singles_division_id = "lmenu_5724"
        

    tourney_titles_xpath = "//*[@id='%s']" % singles_division_id + "//ul//a"
    tourney_titles_href_parsed = xpath_parse(tournament_tree, tourney_titles_xpath + '/@href')
    tourney_titles_parsed = xpath_parse(tournament_tree, tourney_titles_xpath + '/text()')

    tourney_titles_href = regex_strip_array(tourney_titles_href_parsed)
    tourney_titles_href = [{'url':url} for url in tourney_titles_href]
    tourney_titles = regex_strip_array(tourney_titles_parsed)
    tourney = dict(zip(tourney_titles, tourney_titles_href))
    print(f"Found {len(tourney)} {division} Tournaments")
    return tourney
    #array2csv(tourney, "tourney.csv")

def get_tournament_years(tournament_url):
    url_prefix = "https://www.tennis24.com/"
    tournament_tree = html_parse_tree(url_prefix + tournament_url + 'archive')
    #print(etree.tostring(tournament_tree))
    tourney_years_xpath = "//div[@class='leagueTable__season']//div[@class='leagueTable__seasonName']//a"
    tourney_years_href_parsed = xpath_parse(tournament_tree, tourney_years_xpath + '/@href')
    tourney_years_parsed = xpath_parse(tournament_tree, tourney_years_xpath + '/text()')
    tourney_years_href = regex_strip_array(tourney_years_href_parsed)
    tourney_years = regex_strip_array(tourney_years_parsed)
    years = dict(zip(tourney_years, tourney_years_href))
    return years, tournament_url

def new_get_tournament_year_matchs(tournament_year_url):
    url_prefix = "https://www.tennis24.com"
    url = url_prefix + tournament_year_url
    matches = []
    match_tree = html_parse_tree(url)

    tourney_matches_t_ts_xpath = "//li[contains(@class,'bubble')]//a"
    tourney_matches_t_ts_href_parsed = xpath_parse(match_tree, tourney_matches_t_ts_xpath + '/@href')
    tourney_matches_t_ts_href = regex_strip_array(tourney_matches_t_ts_href_parsed)

    #print(tournament_year_url)
    #print(f"{tournament_year_url} found {len(tourney_matches_t_ts_href)} hrefs")
    for href in tourney_matches_t_ts_href:
        tourney_matches_t_value = None
        tourney_matches_ts_value = None

        href_t = re.search(r'(?<=\?t=).*(?=&ts=)', href)
        href_ts = re.search(r'((?<=&ts=).*)', href)
        if href_t.group(0) is not None:
            tourney_matches_t_value = href_t.group(0)
        if href_ts.group(0) is not None:
            tourney_matches_ts_value = href_ts.group(0)
        #print(tourney_matches_t_value, tourney_matches_ts_value)
        if tourney_matches_t_value is not None and tourney_matches_ts_value is not None:
            match_info_url = f"https://d.tennis24.com/x/feed/ss_2_{tourney_matches_t_value}_{tourney_matches_ts_value}_draw_"
            match_tree = curl_html_parse_tree(match_info_url, ['x-fsign: SW9D1eZo'])
            #print(match_info_url)
            #match_tree = html_parse_tree(url, headers={'x-fsign': 'SW9D1eZo'})
            tourney_matches_xpath = '//div[contains(@class,"match-")]'
            #tourney_matches_xpath = '//div[@id="box-table-type--1"]'
            tourney_matches_parsed = xpath_parse(match_tree, tourney_matches_xpath)
            print(len(tourney_matches_parsed))
            matches = matches + tourney_matches_parsed
    match_stats = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = [executor.submit(new_get_match_stats, match) for match in matches]
        for idx, future in enumerate(concurrent.futures.as_completed(futures)):
            print(f"{idx+1}/{len(matches)} complete for {tournament_year_url}")
            try:
                res = future.result()  # This will also raise any exceptions
                if res is not None:
                    match_stats.append(res)
            except:
                continue
    return match_stats

def new_get_match_stats(match):
    match_id = re.search(r'(?<=match-)\w+', match.get('class')).group(0)
    #match_id = xpath_parse_first(match, './/a[contains(@title,"Click for match detail!")]/text()')
    #print(etree.tostring(match))
    player_a = xpath_parse(match, './/span[contains(@class,"participant") and contains(@class,"home")]//span[contains(@class,"name")]/text()')
    player_a_score = xpath_parse(match, './/span[contains(@class,"participant") and contains(@class,"home")]//span[contains(@class,"s")]/text()')
    player_b = xpath_parse(match, './/span[contains(@class,"participant") and contains(@class,"away")]//span[contains(@class,"name")]/text()')
    player_b_score = xpath_parse(match, './/span[contains(@class,"participant") and contains(@class,"away")]//span[contains(@class,"s")]/text()')
    if len(player_a) == 0 or len(player_a_score) == 0 or len(player_b) == 0 or player_b_score == 0:
        return None
    player_a = player_a[0].strip()
    player_a_score = ' '.join(player_a_score)
    player_b = player_b[0].strip()
    player_b_score = ' '.join(player_b_score) 
    winner = xpath_parse(match, './/span[contains(@class,"participant") and contains(@class,"winner")]//span[contains(@class,"name")]/text()')
    if len(winner) > 0:
        winner = winner[0].strip()
    else:
        winner = player_a if int(player_a_score[0]) > int(player_b_score[0]) else player_b
    match_date = xpath_parse(match, './/span[contains(@class,"date")]/text()')
    if len(match_date) > 0:
        match_date = datetime.datetime.fromtimestamp(int(match_date[0])).strftime("%Y%m%d")
    #print(match_id, player_a, player_a_score, player_b, player_b_score, winner,match_date)
    matchs_odds_stats = new_get_matchs_odds(match_id)
    match_dict = dict({
                "match_id":match_id,
                "player_a":player_a,
                "player_a_score":player_a_score,
                "player_b":player_b,
                "player_b_score":player_b_score,
                "winner":winner,
                "match_date":match_date
    })
    match_dict.update(matchs_odds_stats)
    #print(match_dict)
    return match_dict


def new_get_matchs_odds(match_id):
    url = f"https://d.tennis24.com/x/feed/d_od_{match_id}_en_2_eu"
    match_tree = curl_html_parse_tree(url, ['x-fsign: SW9D1eZo'])

    player_a_total = 0
    player_a_opening_odd = 0
    player_a_closing_odd = 0
    player_b_total = 0
    player_b_opening_odd = 0
    player_b_closing_odd = 0
    driver = None
    delay = 8 # seconds
    odds = []
    #print(url)
    #print(tournament_year_match, "trace 0")

    #print(tournament_year_match, "trace 1")
    odds = xpath_parse(match_tree, '//div[@id="block-moneyline-ft"]//table[@id="odds_ml"]//tbody//tr[@class="odd" or @class="even"]//td[contains(@class,"kx")]')
    for odd in odds:
        player = odd.get('onclick')
        odd_value = xpath_parse(odd, './/span')[0].get('alt')
        if odd_value == ':':
            odd_value = xpath_parse(odd, './/span')[0].text
        odd_value = odd_value.replace(':', '')
        #print(player)
        #print(odd_value)
        if odd_value is not None:
            odds_o_c_lines = re.split('\[d\]|\[u\]',odd_value)
            
            #print(odds_o_c_lines)
            if len(odds_o_c_lines) == 1:
                odds_o_c_lines.append(odds_o_c_lines[0])
            #print(odds_o_c_lines)
            if 'block-moneyline_ft_1' in player:
                player_a_opening_odd += float(odds_o_c_lines[0])
                player_a_closing_odd += float(odds_o_c_lines[1])
                player_a_total += 1
            elif 'block-moneyline_ft_2' in player:
                player_b_opening_odd += float(odds_o_c_lines[0])
                player_b_closing_odd += float(odds_o_c_lines[1])
                player_b_total += 1
    if player_a_total == 0:
        player_a_total = 1
    if player_b_total == 0:
        player_b_total = 1
    return dict({"match_id": match_id,
                #"match_stage": match_stage,
                "player_a_opening_odds": player_a_opening_odd / player_a_total,
                "player_a_closing_odds": player_a_closing_odd / player_a_total,
                "player_b_opening_odds": player_b_opening_odd / player_b_total,
                "player_b_closing_odds": player_b_closing_odd / player_b_total,
    })

def parse_player_name(player_name):
    print(player_name)
    first_inital_match = re.search('(?<=\s).*\.$', player_name)
    if first_inital_match != None:
        first_initial = first_inital_match.group(0).strip()
        last_name, country_code = [x.strip().replace('-', ' ') for x in player_name.split(first_initial, maxsplit=1)]
        if len(country_code) == 5:
            country_code = country_code[1:4]
        #last_name, country_code = player_name.split(first_initial, maxsplit=1)
        return first_initial, last_name, country_code
    return '', player_name, ''

def find_match_players_id(division, tournament, year, winning_player, losing_player):
    winner_first_initial, winner_last_name, winner_country_code = parse_player_name(winning_player)
    loser_first_initial, loser_last_name, loser_country_code = parse_player_name(losing_player)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    match_year_file = f"{current_dir}/data/tennis_{division.lower()}/{division.lower()}_matches_{year}.csv"
    data = pd.read_csv(match_year_file, index_col=None, header=0)
    data = data[data['tourney_name'].str.contains(tournament) &
                data['winner_name'].str.contains(winner_last_name) & 
                data['loser_name'].str.contains(loser_last_name)]

    if len(data) > 0:
        print(data.iloc[0]['tourney_id'],
                data.iloc[0]['match_num'],
                data.iloc[0]['winner_id'],
                data.iloc[0]['loser_id'])
        return [
                data.iloc[0]['tourney_id'],
                data.iloc[0]['match_num'],
                data.iloc[0]['winner_id'],
                data.iloc[0]['loser_id']
               ]
    return ['','','','']

def write_years_odds(year_matches, division, year):
    print(f"starting write out of odds of length: {len(year_matches)}")
    year_odds = []
    year_header = ['tourney_id', 'match_num', 'winner_id', 'loser_id',
                   'match_id', 'player_a', 'player_a_score', 'player_b',
                   'player_b_score', 'winner', 'match_date', 'player_a_opening_odds',
                   'player_a_closing_odds', 'player_b_opening_odds', 'player_b_closing_odds',
                   'tournament']
    year_odds.append(year_header)
    for match in year_matches:
        winner = match.get("winner")
        loser = match.get("player_a") 
        if match.get("winner") == match.get("player_a"):
            loser = match.get("player_b")
        found_player_match = find_match_players_id(division, match.get('tournament'), year, winner, loser)
        #print('FOUND:', found_player_match)
        print(found_player_match + list(match.values()))
        year_odds.append(found_player_match + list(match.values()))
    directory = f"./data/{division.lower()}_odds"
    if not os.path.exists(directory):
        os.makedirs(directory)
    array2csv(year_odds, f"{directory}/{division.lower()}_match_odds_{year}.csv")

def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--atp', action='store_true', help='get odds for atp')
    group.add_argument('--wta', action='store_true', help='get odds for wta')
    parser.add_argument('-s', '--start-year', default=2009)
    parser.add_argument('-e', '--end-year', default=datetime.datetime.today().year)
    args = parser.parse_args()
    divisions = []
    if args.atp:
        divisions.append('ATP')
    if args.wta:
        divisions.append('WTA')
    start_year = args.start_year
    end_year = args.end_year
    #print(parse_player_name('Lorenzi P.'))
    #print(get_tournament_year_matchs_odds('g_2_vm7yZM77', 0))
    #find_match_players_id('ATP', 'Adelaide', '2008','Tsonga J-W. (Fra)', 'Gulbis E. (Lat)')
    #sys.exit()
    for division in divisions:
        tournaments = get_tournaments(division)
        years = dict()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_tournament_years, tournaments.get(tournament).get('url')) for tournament in tournaments.keys()]
            for idx, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    res, tournament_url = future.result()  # This will also raise any exceptions

                    tid = next((i for i, d in enumerate(tournaments.keys()) if tournaments[d].get('url') in tournament_url), None)
                    for tournament_year, tournament_year_url in res.items():
                        year = re.search(r'\d+', tournament_year[::-1]).group()[::-1]
                        years.setdefault(year,{})[list(tournaments.keys())[tid]] = {'url': tournament_year_url}
                except:
                    continue
        #print(json.dumps(years, sort_keys=True, indent=4))
        for year in reversed(range(int(start_year), int(end_year)+1)):
            year = str(year)
            if year not in years.keys():
                continue
            year_matches = []
            print(year)
            tournaments = years.get(year).keys()
            for i, tournament in enumerate(tournaments):
                tournament_dict = years.get(year).get(tournament)
                tournament_year_matches = new_get_tournament_year_matchs(tournament_dict.get('url'))
                for tym in tournament_year_matches:
                    tym['tournament'] = tournament
                print(f"{tournament} complete ({i+1}/{len(tournaments)})")
                year_matches = year_matches + tournament_year_matches

            write_years_odds(year_matches, division, year)

if __name__== "__main__":
  main()
