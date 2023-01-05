import requests
import pandas as pd
import sqlite3
import datetime


def LoadNewPlayers(dbpath):

    playerdf = pd.DataFrame(columns=['playerid', 'fullname', 'firstname', 'lastname', 'firstlastname',
                                     'lastfirstname', 'positionname', 'positionabbrev', 'batside', 'pitchhand',
                                     'strikeZoneTop', 'strikeZoneBottom', 'fdplayerid', 'lastupdate'])
    playererordf = pd.DataFrame(columns=['playerid', 'errormessage', 'errorlogdate'])

    # SET SQL CONNECTION
    sqlite_file = dbpath
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    sql = "SELECT PL.playerid \
            FROM \
                (SELECT DISTINCT substr(playerid,3,length(playerid)-2) playerid FROM Batting_BoxScore \
                UNION \
                SELECT DISTINCT substr(playerid,3,length(playerid)-2) playerid FROM Pitching_BoxScore \
                UNION \
                SELECT DISTINCT batterid FROM PlayByPlay \
                UNION \
                SELECT DISTINCT pitcherid FROM PlayByPlay \
                UNION \
                SELECT DISTINCT playerid FROM BattingOrderLineup \
                UNION \
                SELECT DISTINCT homeprobpitcher FROM Games WHERE homeprobpitcher IS NOT NULL \
                UNION \
                SELECT DISTINCT awayprobpitcher FROM Games WHERE awayprobpitcher IS NOT NULL) PL \
            WHERE PL.playerid not in (SELECT DISTINCT playerid from players);"
    c.execute(sql)
    data = c.fetchall()
    # GET BATTERS LOOP
    for row in data:
        playerid = str(row)
        playerid = str(playerid)
        playerid = str(playerid[1:int(len(str(row))-2)])

        # CALL API
        url = 'http://statsapi.mlb.com//api/v1/people/' + playerid
        resp = requests.get(url)
        x = resp.json()

        # PARSE RETURNED JSON
        try:
            x = x['people']
            x = x[0]
            now = str(datetime.datetime.now())
            now = now[0:19]
            playerdf.loc[0] = [x['id'], x['fullName'], x['firstName'], x['lastName'], x['firstLastName'],
                               x['lastFirstName'], x['primaryPosition']['name'], x['primaryPosition']['abbreviation'],
                               x['batSide']['code'], x['pitchHand']['code'], x['strikeZoneTop'], x['strikeZoneBottom'],
                               '', now]

            playerdf.to_sql('Players', conn, if_exists='append')
            conn.commit()
            pass
        except Exception as e2:
            # Load errors into error table
            now = str(datetime.datetime.now())
            errorlist = (playerid, e2, (now[0:19]))
            playererordf.loc[0] = [playerid, str(e2), now[0:19]]
            playererordf.to_sql('ErrorLog_Players', conn, if_exists='append')
            pass

    conn.close()
    now = str(datetime.datetime.now())
    print('Player Data Load Complete ', now[0:19])
