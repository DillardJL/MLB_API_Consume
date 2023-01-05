import sqlite3 as db
import requests


def clearGamesErrors(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "DELETE FROM ErrorLog_Games WHERE errormessage LIKE \'UNIQUE constraint failed:%\';"
		c.execute(sql)
		conn.commit()


def clearBattingOrderErrors(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "DELETE FROM ErrorLog_BattingOrderLineup WHERE errormessage LIKE \'UNIQUE constraint failed:%\';"
		c.execute(sql)
		conn.commit()


def clearBattingBoxErrors(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "DELETE FROM ErrorLog_Batting_BoxScore WHERE errormessage LIKE \'UNIQUE constraint failed:%\';"
		c.execute(sql)
		conn.commit()


def clearPitchingBoxErrors(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "DELETE FROM ErrorLog_Pitching_BoxScore WHERE errormessage LIKE \'UNIQUE constraint failed:%\';"
		c.execute(sql)
		conn.commit()


def clearPlayerErrors(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "DELETE FROM ErrorLog_Players WHERE errormessage LIKE \'UNIQUE constraint failed:%\';"
		c.execute(sql)
		conn.commit()


def clearPlayByPlayErrors(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "DELETE FROM ErrorLog_PlayByPlay WHERE errormessage LIKE \'UNIQUE constraint failed:%\';"
		c.execute(sql)
		conn.commit()


def clearPitchByPitchErrors(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "DELETE FROM ErrorLog_PitchByPitch WHERE errormessage LIKE \'UNIQUE constraint failed:%\';"
		c.execute(sql)
		conn.commit()


def setValidGames(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "UPDATE games set isvalid = 1 WHERE isvalid = 0 \
				AND seriesdescription NOT IN ('MLB All-Star Game','Exhibition Game','Exhibition','Spring Training') \
				AND hometeamid IN (SELECT teamid from teams where leagueid IN (103,104)) \
				AND awayteamid IN (SELECT teamid from teams where leagueid IN (103,104));"
		c.execute(sql)
		conn.commit()


def setUseInPlayCalc(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "UPDATE PlayByPlay SET useincalc = 1 WHERE playresult IN " \
			  "(SELECT DISTINCT playresult FROM PlayByPlay_Legend WHERE useincalc = 1);"
		c.execute(sql)
		conn.commit()


def updatePlayersFanduelID(dbpath):
	with db.connect(dbpath) as conn:
		c = conn.cursor()
		sql = "UPDATE players SET fdplayerid = ( \
						  SELECT substr(id,instr(id,'-')+1) fdplayerid \
						  FROM FanDuelExport ST \
						  WHERE ST.nickname = players.fullname \
						  ) \
			WHERE players.fdplayerid is null "
		c.execute(sql)
		conn.commit()


def rebuildBattingDFS(dbpath, sqlfilepath):
	# SET SQL CONNECTION
	conn = db.connect(dbpath)
	c = conn.cursor()

	fd = open(sqlfilepath + 'SP_BattingDFS.sql')
	sqlFile = fd.read()
	fd.close()
	c.executescript(sqlFile)
	print('Database: Batting DFS Table Rebuilt')


def rebuildPitchingDFS(dbpath, sqlfilepath):
	conn = db.connect(dbpath)
	c = conn.cursor()

	fd = open(sqlfilepath + 'SP_PitchingDFS.sql')
	sqlFile = fd.read()
	fd.close()
	c.executescript(sqlFile)
	print('Database: Pitching DFS Table Rebuilt')

def updateAwayProbPitcher(dbpath):
	# SET SQL CONNECTION
	conn = db.connect(dbpath)
	c = conn.cursor()

	sql = "select * from games where awayprobpitcher = '' and isvalid = 1"
	c.execute(sql)
	data = c.fetchall()

	# LOOP PROCESS FOR GAMES IN DB
	for row in data:
		GameID = str(row)
		GameID = str(GameID)
		GameID = str(GameID[1:int(len(str(row)) - 2)])

		# LOAD DATA FROM LIVE FEED
		LFurl = 'http://statsapi.mlb.com//api/v1.1/game/' + str(GameID) + '/feed/live'
		LFresp = requests.get(LFurl)
		LFdata = LFresp.json()

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
