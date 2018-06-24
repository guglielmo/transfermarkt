import copy
import os

# morph.io requires this db filename, but scraperwiki doesn't nicely
# expose a way to alter this. So we'll fiddle our environment ourselves
# before our pipeline modules load.
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

import scraperwiki
import requests
import lxml.html

starting_leagues = [
    {'name': 'Seria A', 'code': 'IT1', 'slug': 'serie-a'},
    {'name': 'Premiere League', 'code': 'GB1', 'slug': 'premiere-league'},
    {'name': 'La Liga', 'code': 'ES1', 'slug': 'la-liga'},
]
base_url = "https://www.transfermarkt.it"
league_url_tpl = base_url + "/{0}/startseite/wettbewerb/{1}/plus/?saison_id={2}"


def get_teams(conn, league_url, season):
    """return list of teams data (with url)

    :param conn: html session of the connection
    :param league_url: code of the league (es: IT1)
    :param season: season is identified with starting year (es: 2017/2018 = 2017)
    :return: list of dict
    """

    html = conn.get(league_url)

    # Find teams in lague's page
    root = lxml.html.fromstring(html.content)
    links = root.cssselect("#yw1 table.items tbody tr td:nth-child(2) a.vereinprofil_tooltip")

    teams = [
        {
            'name': link.text + " " + str(season) + "/" + str(season+1),
            'url': link.attrib['href'],
            'league': league_url
        }
        for link in links
    ]

    return teams


def get_players(conn, team_url):
    """get players from a team_url

    :param conn: html session of the connection
    :param team_url:
    :return:
    """
    html = conn.get(base_url + team_url)

    # Find players in team's page
    root = lxml.html.fromstring(html.content)
    trs = root.cssselect("#yw1 table.items tbody>tr")

    players = []
    for tr in trs:
        try:
            name_link_el = tr.cssselect("td:nth-child(2) td.hauptlink span.show-for-small a.spielprofil_tooltip")[0]
            birth_date_el = tr.cssselect("td:nth-child(3)")[0]
            img_el = tr.cssselect("td:nth-child(2) table.inline-table img")[0]
        except Exception as e:
            print("Exception {0} found for {1}. Skipping.".format(e, lxml.etree.tostring(tr)))
            continue

        players.append({
            'name': name_link_el.text,
            'url': name_link_el.attrib['href'],
            'birth_date': birth_date_el.text,
            'img_url': img_el.attrib['data-src']
        })

    return players


if __name__ == '__main__':

    with requests.session() as session:

        session.headers.update({
            'User-agent': "Mozilla/5.0"
                          "(Macintosh; Intel Mac OS X 10_11_6) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/55.0.2883.95 Safari/537.36",
            'Connection': 'keep-alive'
        })

        for saison in [2015, 2016, 2017]:
            print("processing season: " + str(saison))

            leagues = copy.deepcopy(starting_leagues)
            for league in leagues:
                league['saison'] = str(saison)
                league['name'] = league.pop('name') + " " + str(saison) + "/" + str(saison+1)
                league['url'] = league_url_tpl.format(
                    league['slug'], league['code'], saison
                )
                league.pop('code')
                league.pop('slug')

            print("  saving leagues")
            scraperwiki.sqlite.save(
                unique_keys=['url'], data=leagues, table_name='leagues'
            )

            for league in leagues:
                print("  processing {0}".format(league['name']))
                league_teams = get_teams(session, league['url'], saison)

                print("    saving teams")
                scraperwiki.sqlite.save(
                    unique_keys=['url'], data=league_teams, table_name='teams'
                )

                for team in league_teams:
                    print("    processing {0}".format(team['name']))
                    team_players = get_players(session, team['url'])

                    print("      saving players")
                    scraperwiki.sqlite.save(
                        unique_keys=['url'], data=team_players, table_name='players'
                    )


