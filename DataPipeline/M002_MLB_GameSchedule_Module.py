import requests
import pandas as pd
import sqlite3
import datetime
from datetime import date, timedelta
from datetime import datetime

def GameSchedule(dbpath, GameScheduleStartDT, GameScheduleEndDT):

	# SET SQL CONNECTION
	sqlite_file = dbpath
	conn = sqlite3.connect(sqlite_file)
	c = conn.cursor()

	# VARIABLES
	ldr = pd.DataFrame(columns=['gameid', 'season', 'gamedate', 'gmtgamedate', 'gamestate', 'awayteamid', 'hometeamid',
								'venueid', 'gamelocaltime', 'gamelocalampm', 'gamedaynight', 'doubleheader',
								'normalgame', 'seriesdescription', 'gamesinseries', 'seriesgamenumber',
								'awayprobpitcher', 'homeprobpitcher', 'temperature', 'windspeed', 'winddirection',
								'pressure', 'iswxfinal', 'islineupfinal', 'isbatboxfinal', 'ispitchboxfinal',
								'ispbpfinal', 'isvalid', 'lastupdate'])
	gameerrordf = pd.DataFrame(columns=['GameID', 'ErrorMessage', 'ErrorLogDate'])

	# Dynamically pass in Date!
	start_date = datetime.strptime(GameScheduleStartDT, '%Y-%m-%d').date()
	# start_date = date(2020, 6, 22)
	end_date = datetime.strptime(GameScheduleEndDT, '%Y-%m-%d').date()
	# end_date = date(2020, 6, 23)
	delta = timedelta(days=1)
	daygamecount = 0

	#LOOP PROCESS FOR DATES IN RANGE
	while start_date <= end_date:
		game_date = start_date.strftime("%m/%d/%Y")
		start_date += delta
		url = 'http://statsapi.mlb.com//api/v1/schedule/games/?sportId=1&date=' + game_date
		resp = requests.get(url)
		data = resp.json()

		if 'dates' in data and data['totalGames'] > 0:
			daygamecount = data['totalGames']
			data = data['dates']
			x = list()
			for i in data:
				x = x, (i['games'])

			for i in x:
				x = x, (i)
			x = list(x)
			x = pd.DataFrame(x)
			x = pd.DataFrame(x.iloc[[1]])

		# loop through data frame columns shape[1] of the df and print the game data)
		for i in range(daygamecount):
			ld = x.loc[1][i]
			now = str(datetime.now())
			try:
				ldr.loc[0] = [ld['gamePk'], ld['season'], ld['officialDate'], ld['gameDate'], ld['status']['detailedState'],
							  ld['teams']['away']['team']['id'], ld['teams']['home']['team']['id'], ld['venue']['id'],
							  '', '', ld['dayNight'], ld['doubleHeader'], ld['ifNecessaryDescription'],
							  ld['seriesDescription'], '', ld['seriesGameNumber'], '', '', '', '', '', '', '0', '0',
							  '0', '0', '0', '0', now[0:19]]

				pass
			except Exception as e1:
				# Load errors into error table
				now = str(datetime.now())
				errorlist = (ld['gamePk'], e1, (now[0:19]))
				gameerrordf.loc[0] = [ld['gamePk'], str(e1), now[0:19]]
				gameerrordf.to_sql('ErrorLog_Games', conn, if_exists='append')
				pass

			try:
				ldr.to_sql('Games', conn, if_exists='append')
				conn.commit()
				print('GameID :', ld['gamePk'], ' load success')
				pass

			except Exception as e2:
				#Load errors into error table
				now = str(datetime.now())
				errorlist = (ld['gamePk'], e2, (now[0:19]))
				gameerrordf.loc[0] = [ld['gamePk'], str(e2), now[0:19]]
				gameerrordf.to_sql('ErrorLog_Games', conn, if_exists='append')
				pass
	conn.close()

	now = str(datetime.now())
	print('Game Schedule Data Load Complete ', now[0:19])
