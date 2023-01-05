import requests
import sqlite3 as db
import sqlite3
import datetime

def GameAttributeUpdate(dbpath):

    # SET SQL CONNECTION
    sqlite_file = dbpath
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    sql = "select distinct gameid FROM games where gamestate is null or gamestate <> \'Final\';"
    c.execute(sql)
    data = c.fetchall()

    # LOOP PROCESS FOR GAMES IN DB
    for row in data:
        GameID = str(row)
        GameID = str(GameID)
        GameID = str(GameID[1:int(len(str(row))-2)])

    # LOAD DATA FROM LIVE FEED
        LFurl = 'http://statsapi.mlb.com//api/v1.1/game/' + str(GameID) + '/feed/live'
        LFresp = requests.get(LFurl)
        LFdata = LFresp.json()
    # LOAD DATA FROM CONTEXT METRICS
        CMurl = 'https://statsapi.mlb.com/api/v1/game/' + str(GameID) + '/contextMetrics'
        CMresp = requests.get(CMurl)
        CMdata = CMresp.json()

    # Loop Variables and update database
    ##Season
        try:
            Season = CMdata['game']['season']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set season = " + Season + " where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
                pass
        except:
            pass
    ##GameDate
        try:
            GameDate = CMdata['game']['officialDate']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set gamedate = \'" + str(GameDate) + "\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
                pass
        except:
            pass
    ##GMTGameDate
        try:
            GMTGameDate = CMdata['game']['gameDate']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set gmtgamedate = \'" + GMTGameDate + "\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
                pass
        except:
            pass
    ##GameState
        try:
            GameState = CMdata['game']['status']['detailedState']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set gamestate = \'" + GameState + "\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
                pass
        except:
            pass

    ##AwayTeamID
        try:
            AwayTeamID = CMdata['game']['teams']['away']['team']['id']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set awayteamid = " + str(AwayTeamID) + " where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
                pass
        except:
            pass
    ##HomeTeamID
        try:
            HomeTeamID = CMdata['game']['teams']['home']['team']['id']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set hometeamid = " + str(HomeTeamID) + " where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
                pass
        except:
            pass
    ##VenueID
        try:
            VenueID = CMdata['gameData']['venue']['id']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set venueid = " + str(VenueID) + " where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
                pass
        except:
            pass
    ##LocalTime
        try:
            LocalTime = LFdata['gameData']['datetime']['time']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set gamelocaltime = \'" + LocalTime+"\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
                pass
        except:
            pass
    ##LocalTimeAMPM
        try:
            LocalTimeAMPM = LFdata['gameData']['datetime']['ampm']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set gamelocalampm = \'" + LocalTimeAMPM + "\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass

    ##DayNight
        try:
            DayNight = CMdata['game']['dayNight']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set gamedaynight = \'" + DayNight + "\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass
    ##DoubleHeader
        try:
            DoubleHeader = CMdata['game']['doubleHeader']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set doubleheader = \'" + DoubleHeader + "\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass
    ##NormalGame
        try:
            NormalGame = CMdata['game']['ifNecessaryDescription']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set normalgame = \'" + NormalGame + "\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass
    ##SeriesDescription
        try:
            SeriesDescription = CMdata['game']['seriesDescription']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set seriesdescription = \'" + SeriesDescription + "\' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass
    ##GamesInSeries
        try:
            GamesInSeries = CMdata['game']['gamesInSeries']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set gamesinseries = " + str(GamesInSeries) + " where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass
    ##SeriesGameNumber
        try:
            SeriesGameNumber = CMdata['game']['seriesGameNumber']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set seriesgamenumber = " + str(SeriesGameNumber) + " where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass
    ##AwayProbPitcher
        try:
            AwayProbPitcher = LFdata['gameData']['probablePitchers']['away']['id']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set awayprobpitcher = " + str(AwayProbPitcher) + " where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass
    ##HomeProbPitcher
        try:
            HomeProbPitcher = LFdata['gameData']['probablePitchers']['home']['id']
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set homeprobpitcher = " + str(HomeProbPitcher) + " where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except:
            pass
    ##lastupdate
        try:
            now = str(datetime.datetime.now())
            now = now[0:19]
            with db.connect(dbpath) as conn1:
                cur1 = conn1.cursor()
                upsql = "update games set lastupdate = '" + now + "' where gameid=" + str(GameID) + ";"
                cur1.execute(upsql)
                conn1.commit()
        except Exception as e2:
            #print(e2)
            pass

    conn.close()
    now = str(datetime.datetime.now())
    print('Game Attributes Update Complete ', now[0:19])
