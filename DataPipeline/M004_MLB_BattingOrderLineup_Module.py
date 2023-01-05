import requests
import pandas as pd
import sqlite3 as db
import datetime

def battingOrderLineup(dbpath):

	# VARIABLES
	aldr = pd.DataFrame(columns=['playergamekey', 'gameid', 'playerid', 'batorder', 'teamid', 'lastupdate'])
	hldr = pd.DataFrame(columns=['playergamekey', 'gameid', 'playerid', 'batorder', 'teamid', 'lastupdate'])
	battingerrordf = pd.DataFrame(columns=['playerID', 'GameID', 'ErrorMessage', 'ErrorLogDate'])



	# SET SQL CONNECTION
	with db.connect(dbpath) as conn:
		cur = conn.cursor()
		sql = "SELECT distinct gameid FROM Games WHERE isvalid = 1 and islineupfinal = 0"
		cur.execute(sql)
		data = cur.fetchall()

	# LOOP OVER GAMES FROM DB
	for row in data:
		try:
			gameid = str(row)
			gameid = str(gameid)
			gameid = str(gameid[1:int(len(str(row))-2)])

			# DELETE EXISTING LINEUP DATA
			sqldel = "DELETE FROM BattingOrderLineup WHERE gameid = " + gameid + ";"
			cur.execute(sqldel)
			conn.commit()

			# LOAD LINEUP DATA FROM URL
			LUPurl = 'https://statsapi.mlb.com/api/v1/schedule?gamePk=' + gameid + '&language=en&hydrate=lineups'
			LUPresp = requests.get(LUPurl)
			LUPdata = LUPresp.json()

			# AWAY LOOP
			abo = 0
			x = 0
			while x < 9:
				abo = abo + 1
				now = str(datetime.datetime.now())
				now = now[0:19]
				playerid = LUPdata['dates'][0]['games'][0]['lineups']['awayPlayers'][x]['id']
				playergamekey = (str(playerid) + '_' + str(gameid))
				teamid = LUPdata['dates'][0]['games'][0]['teams']['away']['team']['id']

				# POPULATE AWAY DATAFRAME
				aldr.loc[0] = [playergamekey, gameid, playerid, abo, teamid, now]

				# LOAD AWAY DATAFRAME TO DB
				try:
					aldr.to_sql('BattingOrderLineup', conn, if_exists='append')
					conn.commit()
					pass

				except Exception as e:
					# LOAD ERRORS INTO BATTINGORDERLINEUP ERROR TABLE
					battingerrordf.loc[0] = [playerid, gameid, str(e), now]
					battingerrordf.to_sql('ErrorLog_BattingOrderLineup', conn, if_exists='append')
					#print(str(e))
					pass
				x = x + 1

			# HOME LOOP
			hbo = 0
			i = 0
			while i < 9:
				hbo = hbo + 1
				now = str(datetime.datetime.now())
				now = now[0:19]
				playerid = LUPdata['dates'][0]['games'][0]['lineups']['homePlayers'][i]['id']
				playergamekey = (str(playerid) + '_' + str(gameid))
				teamid = LUPdata['dates'][0]['games'][0]['teams']['home']['team']['id']

				#POPULATE HOME DATAFRAME
				hldr.loc[0] = [playergamekey, gameid, playerid, hbo, teamid, now]

				# LOAD HOME DATAFRAME TO DB
				try:
					hldr.to_sql('BattingOrderLineup', conn, if_exists='append')
					conn.commit()
					pass

				except Exception as e:
					# LOAD ERRORS INTO BATTINGORDERLINEUP ERROR TABLE
					battingerrordf.loc[0] = [playerid, gameid, str(e), now]
					battingerrordf.to_sql('ErrorLog_BattingOrderLineup', conn, if_exists='append')
					#print(str(e))
					pass
				i = i + 1
				pass
	# #CHECK GAMESTATE - IF GAME IS FINAL, SET islineupfinal = 1
			cur1 = conn.cursor()
			lsc_sql = "SELECT gamestate FROM Games WHERE gameid = " + gameid + ";"
			cur1.execute(lsc_sql)
			lsc = cur1.fetchall()
			lsc = str(lsc)
			lsc = str(lsc[3:8])
			fc = str('Final')
			if lsc == fc:
				cur1 = conn.cursor()
				sql_sls = "UPDATE GAMES SET islineupfinal = 1 WHERE gameid = " + gameid + ";"
				cur1.execute(sql_sls)
				conn.commit()
		except:
			pass

	now = str(datetime.datetime.now())
	print('Batting Order Lineup Update Complete ', now[0:19])
