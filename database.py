import sqlite3
from typing import *
import re
import os
from typing import List
from jellyfish import levenshtein_distance
from nltk import word_tokenize
from datetime import datetime
from datetime import date as date_object


class Database:
    """
    Parent class with methods to establish a connection to SQLite database and other
    utility methods
    """
    def __init__(self, db_address : str):
        self.conn = None
        self.cursor = None
        self.db_address : str = db_address

    def establishConnection(self) -> None:
        """
        Setup of the current SQLite database connection
        """
        try:
            self.conn = sqlite3.connect(self.db_address)
        except sqlite3.Error as e:
            print("SQLite Error: {}".format(e))

    def setCursor(self) -> None:
        """
        Sets the cursor of the database to allow execution of SQLite commands and queries
        """
        try:
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print("SQLite Error: {}".format(e))

    def commitChanges(self) -> None:
        self.conn.commit()

    def closeDB(self) -> None:
        self.conn.close()


class DatabaseInitialiser(Database):
    """
    Child class which encapsulates all methods to create all inter-linked tabled in a relational SQLite table.
    Reads CSV files of FIFA player data and Premier League Match data before inserting them into their respective tables
    """
    def __init__(self, db_address : str, player_datasets : List, lineups_datasets : List):
        super().__init__(db_address=db_address)
        self._player_datasets : List = player_datasets
        self._lineups_datasets : List = lineups_datasets
        self._player_values: List = []
        self._lineup_values : List = []
        self._existing_clubs : Dict = {}
        self._club_ids : dict = {}
        self._name_ids : dict = {}

    def collectPlayers(self) -> None:
        """
        Adds a list of all players in each CSV file to player_values list to form a multi-dimensional array
        """
        for dataset in self._player_datasets:
            self._player_values += (self.readCSV(dataset))

    def collectLineups(self) -> None:
        """
        Adds each lineup of each game in each CSV file to lineup_values list to form a 2-dimensional array
        """
        for dataset in self._lineups_datasets:
            self._lineup_values += (self.readCSV(dataset))

    def collectClubs(self) -> None:
        """
        Finds and stores every unique club and their season across all seasons provided. Stored in a dictionary with
        KEY: season, VALUE: List [club_name, ]
        """
        for player in self._player_values:  #Iterates through each extracted row
            club_name : str = player[4]
            season    : str = player[5]
            if (season in self._existing_clubs.keys()) and not(club_name in self._existing_clubs[season]):  #If the season is already a KEY and a club_name not present in a VALUE
                self._existing_clubs[season].append(club_name)
            elif not(season in self._existing_clubs.keys()):  #If the season KEY doesn't exist
                self._existing_clubs[season] = [club_name]

    def assignClubIDs(self) -> None:
        """
        Sets a unique integer ID to every club in existing_clubs by creating a new dictionary.
        Held in a Dictionary with a tuple key of (season, club)
        and value: current_id
        """
        current_id : int = 1  #ClubID given in order of appearance
        for season in self._existing_clubs:  #Iterates over keys
            for club in self._existing_clubs[season]:
                self._club_ids[(season, club)] = current_id
                current_id += 1

    def createPlayersTable(self):
        """
        Executes an SQLite command which creates a table called Players with a primary key of playerID and the various
        columns of the attributes each player. The clubID is a foreign key which references the Clubs table. Forming a
        1 to 1 relationship
        """
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Players (                     \
                              playerID			        INT,                            \
                              player_name		        TEXT,                           \
                              player_name_long	        TEXT,                           \
                              player_position	        TEXT,                           \
                              player_rating		        INT,                            \
                              clubID			        INT,                            \
                              player_season		        TEXT,                           \
                              PRIMARY KEY (playerID),                                   \
                              FOREIGN KEY (clubID) REFERENCES Clubs(clubID)             \
                            );""")
        self.commitChanges()

    def createClubsTable(self):
        """
        Executes an SQLite command which creates a table called Clubs with a primary key of clubID and the various columns
        of the attributes of each player.
        """
        self.cursor.execute("CREATE TABLE IF NOT EXISTS Clubs (                         \
                                clubID  	            INT,                            \
                                club_name		        TEXT,                           \
                                club_season	            TEXT,                           \
                                PRIMARY KEY (clubID)                                    \
                            );")
        self.commitChanges()

    def createClubMatchTable(self):
        """
        Executes an SQLite command which creates an intermediary table between table Clubs and the table Match to handle
        the many to many relationship as a match has 2 teams.
        clubID is a foreign key of the table Clubs.
        matchID is a foreign key of the table Match.
        """
        self.cursor.execute("CREATE TABLE IF NOT EXISTS ClubMatch (                     \
                                clubID    	            INT,                            \
                                matchID   	            INT,                            \
                                FOREIGN KEY (clubID) REFERENCES clubs(clubID),          \
                                FOREIGN KEY (matchID) REFERENCES Matches(matchID)       \
                                );")
        self.commitChanges()

    def createMatchesTable(self):
        """
        Executes an SQLite command which creates a table called Matches with a primary key of matchID. The foreign keys
        home_clubID and away_clubID both reference the intermediary table: ClubMatch. Match has a one to one relationship
        with lineups, hence the foreign key lineupID referencing the table Lineups.
        """
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Matches (                     \
                              matchID			        INT,                            \
                              match_season		        TEXT,                           \
                              home_clubID		        INT,                            \
                              away_clubID		        INT,                            \
                              score				        TEXT,                           \
                              lineupID		            INT,                            \
                              match_date                TEXT,                           \
                              PRIMARY KEY (matchID),                                    \
                              FOREIGN KEY (home_clubID) REFERENCES ClubMatch(clubID),   \
                              FOREIGN KEY (away_clubID) REFERENCES ClubMatch(clubID),   \
                              FOREIGN KEY (lineupID) REFERENCES Lineups(lineupID)       \
                            );""")
        self.commitChanges()

    def createLineupTable(self):
        """
        Executes an SQLite command which creates a table called Lineups with a primary key of lineupID and the various columns
        of the playerID of each player. Each playerID is a foreign key which references the Players table.
        """
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS Lineups (                      \
                              lineupID			INT,                                     \
                              playerID_H1		INT,                                     \
                              playerID_H2		INT,                                     \
                              playerID_H3		INT,                                     \
                              playerID_H4		INT,                                     \
                              playerID_H5		INT,                                     \
                              playerID_H6		INT,                                     \
                              playerID_H7		INT,                                     \
                              playerID_H8		INT,                                     \
                              playerID_H9		INT,                                     \
                              playerID_H10		INT,                                     \
                              playerID_H11		INT,                                     \
                              playerID_A1		INT,                                     \
                              playerID_A2		INT,                                     \
                              playerID_A3		INT,                                     \
                              playerID_A4		INT,                                     \
                              playerID_A5		INT,                                     \
                              playerID_A6		INT,                                     \
                              playerID_A7		INT,                                     \
                              playerID_A8		INT,                                     \
                              playerID_A9		INT,                                     \
                              playerID_A10		INT,                                     \
                              playerID_A11		INT,                                     \
                              PRIMARY KEY (lineupID),                                    \
                              FOREIGN KEY (playerID_H1) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H2) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H3) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H4) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H5) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H6) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H7) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H8) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H9) REFERENCES Players(playerID),    \
                              FOREIGN KEY (playerID_H10) REFERENCES Players(playerID),   \
                              FOREIGN KEY (playerID_H11) REFERENCES Players(playerID)    \
                            );""")
        self.commitChanges()

    def readCSV(self, dataset_name : str) -> List:
        """
        Polymorphic method utilised by both the player and lineup inserting methods
        :param dataset_name: string of the name of the csv file in directory of this program
        :return: 2D-List of each row and their attributes on each column
        """
        entries : List[List,] = []
        assert os.path.isfile(dataset_name), "CSV does not exist!"  # asserts to see if CSV exists, otherwise exception
        with open(mode="r", file=dataset_name, encoding='utf-16') as file:
            for row in file:
                try:
                    formatted_row : str = re.sub("\n", "", row)  #Removes newline characters
                    attributes : List[str] = formatted_row.split(",")  #Creates a list on the string where each ',' is used
                    entries.append(attributes)  #The attribute List is appended to entries to form a 2D array
                except Exception as e:
                    print("Error reading CSV files, likely poor formatting: {}".format(e))
        return entries

    def insertPlayers(self) -> None:
        """
        Executes an insert SQLite command to add each player held in the array player_values.
        """
        player_id = 1
        for player in self._player_values:
            self.cursor.execute("""
                                INSERT INTO Players(playerID, player_name, player_name_long, player_position, player_rating, clubid, player_season)
                                VALUES (?,?,?,?,?,?,?);
                                """, (player_id, player[0],player[1],player[2],player[3],self._club_ids[(player[5], player[4])],player[5]))
            """A dictionary is created to record each ID given to the player:
              KEY : Tuple (Club name, season)
              VALUE : List [Tuple (playerID, Short player name, Long player name)]"""
            if (player[4], player[5]) in self._name_ids.keys():  #if the key:value pair already exists
                self._name_ids[(player[4], player[5])].append((player_id, player[0], player[1]))
            else:  #New key:value pair created
                self._name_ids[(player[4], player[5])] = [(player_id, player[0], player[1])]
            player_id += 1
        self.commitChanges()

    def insertClubs(self) -> None:
        """
        Executes an insert SQLite command to add each club held in the class attribute club_ids (type : List)
        """
        for club in self._club_ids:
            self.cursor.execute("""INSERT INTO Clubs(clubid, club_name, club_season) \
                                Values (?,?,?);""",(self._club_ids[club], club[1], club[0]))
        self.commitChanges()

    def insertClubMatch(self, clubID : int, matchID : int):
        """
        :param clubID: integer of the unique primary key for the Club
        :param matchID: integer of the unique primary key for the Match
        Used to create SQLite table - ClubMatch which handles the many-many relationship between Clubs and Matches
        """
        self.cursor.execute("""INSERT INTO ClubMatch (clubID, matchID) \
                            VALUES (?,?);""", (clubID, matchID))

    def insertMatches(self) -> None:
        """
        Iterates through each match in the List - lineup_values in the constructor, formats data where necessary.
        Also, gathers the relevant clubID, date and season before inserting into the existing table - Matches
        """
        current_id = 1
        for match in self._lineup_values:
            home_name, away_name, score, date_unformatted, *_ = match  #'*_' relates to the tail of the list, allows partial unpacking
            season  : str = self.dateToSeason(match[3])
            ISO_date: str = datetime.strptime(match[3], '%d %B %Y').strftime('%Y-%m-%d')  #Adjusts formatting to comply with SQLite date standard
            try:
                home_team_id = self._club_ids[(season, match[0])]  #The club ID is looked up based on the season name and club name.
            except KeyError:  #If the club can't be located because of name. String similarity is used.
                home_team_id = self.findClubLevenshtein(find_id=True, entered_season=season, entered_club=match[0])
            try:
                away_team_id = self._club_ids[(season, match[1])]
            except KeyError:
                away_team_id = self.findClubLevenshtein(find_id=True, entered_season=season, entered_club=match[1])

            self.cursor.execute("""INSERT INTO Matches(matchID, match_season, home_clubID, away_clubID, score, lineupID, match_date)
                                    VALUES (?,?,?,?,?,?,?)""",
                                (current_id, season, home_team_id, away_team_id, match[2], current_id, ISO_date))
            self.commitChanges()
            self.insertClubMatch(home_team_id, current_id)  #Adds values to ClubMatch to handle Many-Many
            self.insertClubMatch(away_team_id, current_id)
            self.insertLineups(current_id, match)
            current_id += 1

    def insertLineups(self, current_id : int, match : List):
        """
        :param current_id: integer of the current match/lineup primary key
        :param match:
        This is a one-one relationship between Matches and Lineups thus this method is called on each iteration in
        insertMatches() to add each lineup to the table - Lineups.
        """
        home_lineup : List = match[4:15]
        date_season: str = self.dateToSeason(match[3])
        try:
            home_team_ids : List[Tuple,] = self._name_ids[(match[0]), date_season]  #List of Tuples of each player (ID, shortname, longname). match[0] is the club name
        except KeyError:  #If club can't be found, string similarity is used
            home_team_ids : List[Tuple,] = self._name_ids[self.findClubLevenshtein(find_id=False, entered_season=date_season, entered_club=match[0]), date_season]

        away_lineup : List = match[15:]
        try:
            away_team_ids : List[Tuple,] = self._name_ids[(match[1]), date_season]  #match[1] is the club name
        except KeyError:
            away_team_ids : List[Tuple,] = self._name_ids[self.findClubLevenshtein(find_id=False, entered_season=date_season, entered_club=match[1]), date_season]
        home_values = []  #Holds
        away_values = []
        assert len(home_lineup) == len(away_lineup)
        for h_player, a_player in zip(home_lineup, away_lineup):  #Simultaneous iteration over each List of lineup names
            for home_id in home_team_ids:  #Iteration over each known player from that club, home_id: (ID, shortname, longname)
                if h_player in home_id:  #If the player name appears in either long or short name
                    h_player_id : int = home_id[0]
                    home_values.append(h_player_id)
                    home_team_ids = list(filter(lambda player : player != home_id, home_team_ids))  #Player removed before next iteration
                    home_lineup = list(filter(lambda player: player != h_player, home_lineup))
                    break  #player found, no more iteration needed
            for away_id in away_team_ids:
                if a_player in away_id:
                    a_player_id : int = away_id[0]
                    away_values.append(a_player_id)
                    away_team_ids = list(filter(lambda player: player != away_id, away_team_ids))
                    away_lineup = list(filter(lambda player: player != a_player, away_lineup))
                    break

        if len(home_lineup) > 0: # if elements remaining, n-gram string similarity is used
            h_similar_player = self.compareBigrams(players=home_lineup, team_ids=home_team_ids)
            home_values += h_similar_player

        if len(away_lineup) > 0:
            a_similar_player = self.compareBigrams(players=away_lineup, team_ids=away_team_ids)
            away_values += a_similar_player

        values : List = [current_id] + home_values + away_values
        self.cursor.execute("""INSERT INTO Lineups (lineupID,playerID_H1, playerID_H2, playerID_H3, playerID_H4, 
                                    playerID_H5, playerID_H6, playerID_H7, playerID_H8, playerID_H9, playerID_H10, 
                                    playerID_H11, playerID_A1, playerID_A2, playerID_A3, playerID_A4, playerID_A5, 
                                    playerID_A6, playerID_A7, playerID_A8, playerID_A9, playerID_A10, playerID_A11)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""", values)
        self.commitChanges()

    def dateToSeason(self, match_date : str) -> str:
        """
        Converts a date related to a match to a string of the current season
        :param match_date: string format of the match date
        :return: string of the corresponding season
        """
        season_names : List = ['2012/2013', '2013/2014', '2014/2015', '2015/2016', '2016/2017', '2017/2018', '2018/2019', '2019/2020']
        """Creates a list of Tuples (season name, datetime object of last day of season)"""
        season_boundaries : List[Tuple] = [(date_object(year, 5, 30), season_years) for year, season_years in zip(range(2013, 2021), season_names)]
        date_formatted = datetime.strptime(match_date, "%d %B %Y").date()  #Converts the method parameter from string to date object
        for season in season_boundaries:
            date, years = season  #Unpacks each tuple
            if date_formatted < date:  #If the date from the parameter is less than the current iteration of the season
                return years

    def findClubLevenshtein(self, find_id : bool, entered_season : str, entered_club : str) -> Union[int, str]:
        """
        Applies Levenshtein string editing distance to get either the clubID or the name, depends on the boolean input
        find_up
        :param find_id: Boolean which determines if the club ID or name is returned
        :param entered_season: string of the season of the club
        :param entered_club: string of the club name
        :return: [1] integer of the clubID OR [2] name of the most similar club
        """
        greatest_similarity = (float('inf'), int, str)
        for (season, clubname), id in self._club_ids.items():  #Iteration through each key value pair
            if entered_season == season:
                distance : int = levenshtein_distance(clubname, entered_club)  #Finds levenshtein distance between param and stored name
                if distance < greatest_similarity[0]:  #If distance of this iteration is lowest so far
                    greatest_similarity : Tuple = (distance, id, clubname)  #New most similar club recorded
        if find_id:  #Returns the ID
            return greatest_similarity[1]
        else:  #Returns the club name
            return greatest_similarity[2]

    def generateBigrams(self, player : str) -> List[str]:
        """
        :param player: string of player name
        :return: A list of bigrams
        Filters string for irrelevant characters then separates each bigram in the word.
        Part of the n-gram string similarity algorithm
        """
        formatted_player = "".join(char for char in player if char != '.')  #remove full-stop
        player_names = word_tokenize(formatted_player)  #group by words
        significant_names = [name for name in player_names if len(name) > 1]  #Ignores name initials eg. T Cairney -> Cairney
        player_bigrams = []
        for n in significant_names:
            player_bigrams += [first + second for first, second in zip(n[:], n[1:])]  #Generates a list of bigrams
        return player_bigrams

    def compareBigrams(self, players : List, team_ids : List[Tuple,]) -> List:
        """
        :param players: List the remaining players in the lineup which returned a KeyError
        :param team_ids: List of Tuples of each player name and their playerID
        :return: List of the IDs of the similar players
        Finds the Jaccard co-efficient to find the most similar string held the a club
        """
        similar_players = []
        for player in players:
            player_bigram : List = self.generateBigrams(player)  #Generates bigrams of parameter player
            greatest_similarity : (int, int) = (0, 0)
            for club_player in team_ids:  #club_player is Tuple[id, shortname, longname]
                id, shortname, longname = club_player
                short_club_player_bigrams : List = self.generateBigrams(shortname)  #List of bigrams in the short name of the current stored club in iteration
                long_club_player_bigrams  : List = self.generateBigrams(longname)
                common_short_bigrams = len([bigram for bigram in short_club_player_bigrams if bigram in player_bigram])  #UNION of bigrams between current short name and param name
                common_long_bigrams  = len([bigram for bigram in long_club_player_bigrams if bigram in player_bigram])
                if common_short_bigrams <= common_long_bigrams and common_long_bigrams > greatest_similarity[0]:  #Compares current similarity
                    greatest_similarity = (common_long_bigrams, id)  #updated similarity
                elif common_short_bigrams > common_long_bigrams and common_short_bigrams > greatest_similarity[0]:
                    greatest_similarity = (common_short_bigrams, id)
            similar_players.append(greatest_similarity[1])  #Adds shortname to the list of similar players
            """team_ids is filtered to remove the current player to prevent multiple players matching to same player"""
            team_ids = list(filter(lambda player: player[0] != greatest_similarity[1], team_ids))
        return similar_players


class DatabaseSelector(Database):
    """
    This subclass of Database, encapsulates all of the methods related to SQLite
    SELECT queries to extract values from the database
    """
    def __init__(self, db_address):
        super().__init__(db_address=db_address)  #inherits the constructor of Database
        assert os.path.isfile(db_address)  # asserts to see if Database exists, otherwise exception

    def selectSeasons(self) -> List:
        """
        Executes a select query to extract every unique season in the Clubs table.
        This is used later for the GUI to allow the user to select an available season
        :return: List of unique seasons as strings.
        """
        self.cursor.execute("""SELECT DISTINCT club_season FROM Clubs ORDER BY club_season DESC;""")
        return [season[0] for season in self.fetchSelection()]

    def selectClubNames(self, season : str) -> List[Tuple[str, int],]:
        """
        Executes a select query to extract the name of every club and its ID.
        This is used later for the GUI to allow the user to select a club through its
        name.
        :param season: String of the current season
        :return: List of Tuples in the form [(club_name, clubID),]
        """
        self.cursor.execute("""SELECT club_name, clubID FROM Clubs WHERE club_season = ?;""", (season,))
        return self.fetchSelection()

    def selectPlayersFromClub(self, clubID : int) -> List[str,]:
        """
        Executes a select query to recall the name of every player within the club.
        This is used in the GUI to display to the user every available player in a
        drop-down box
        :param clubID: Integer of the club ID
        :return: List of strings [player_name,]
        """
        self.cursor.execute("""SELECT player_name FROM Players WHERE clubID = ?;""", (clubID,))
        return self.fetchSelection()

    def selectPlayerData(self, player_name : str, player_season : str) -> List[Tuple,]:
        """
        Executes a select query to collate all information of a single player based
        off their name, this is used in the GUI to relay player data for the model.
        The user selects a player name without a primary key - playerID attached hence
        the purpose of this method
        :param player_name: string of the name of the player
        :param player_season: string of the name of the season
        :return: List of tuples containing the data from each column
        """
        self.cursor.execute("""SELECT * FROM Players WHERE player_name = ? AND player_season = ?""",
                            (player_name, player_season,))
        return self.fetchSelection()

    def selectPlayer(self, playerID : int) -> List:
        """
        Similar to the method selectPlayerData, instead searches using the primary key,
        playerID. This is heavily used when creating a training and testing set.
        :param playerID: Integer
        :return: List of tuples containing the data from each column
        """
        self.cursor.execute("""SELECT * FROM Players WHERE playerID = ?""",(playerID,))
        return self.fetchFirstSelection()

    def selectLineup(self, lineupid : int) -> Tuple[List, List]:
        """
        A key method for creating the training set. Uses the primary key -lineupID
        to extract the home team and then the away team.
        :param lineupid: Integer Primary Key of Line-up table
        :return: A Tuple containing a list of player IDs for the home and away sides
        """
        self.cursor.execute("""SELECT playerID_H1, playerID_H2, playerID_H3, playerID_H4, playerID_H5, playerID_H6,playerID_H7, playerID_H8, playerID_H9,playerID_H10, playerID_H11 
                                FROM Lineups WHERE lineupID = ?""", (lineupid,))
        home_lineup = self.fetchFirstSelection()
        self.cursor.execute("""SELECT playerID_A1, playerID_A2, playerID_A3, playerID_A4, playerID_A5, playerID_A6,playerID_A7, playerID_A8, playerID_A9,playerID_A10, playerID_A11 
                                FROM Lineups WHERE lineupID = ?""", (lineupid,))
        away_lineup = self.fetchFirstSelection()
        return (home_lineup, away_lineup)

    def getMatches(self) -> List[Tuple,]:
        """
        Executes a SELECT query to extract every match present within the database
        with all columns
        :return:
        """
        self.cursor.execute("""SELECT * FROM Matches;""")
        matches = self.fetchSelection()
        return matches

    def getRecentMatches(self, match_date : str, clubID : int) -> List[Tuple[int, str, int],]:
        """
        Key method to extract the match data over the last month for recent form to
        be calculated in the Team class.
        :param match_date: ISO date format of current match
        :param clubID: INTEGER of clubID
        :return: List of tuples containing home and away team IDs with the score
        """
        self.cursor.execute("""SELECT home_clubID, score, away_clubID
                                FROM Matches
                                WHERE (home_clubID = ? OR away_clubID = ?) AND (match_date BETWEEN DATE(?, '-1 month') AND DATE(?, '-1 day'));
                            """, (clubID, clubID, match_date, match_date))
        return self.fetchSelection()

    def fetchSelection(self)  -> List:
        """
        This method is used in every SELECT operation to retrieve every row selected
        by the query
        :return: List of each value (multi-column selections use Tuple) in the relevant
        rows
        """
        table_rows = self.cursor.fetchall()
        return [row for row in table_rows]

    def fetchFirstSelection(self) -> List:
        """
        This method is used in every SELECT operation to retrieve only the first row
        selected by the query
        :return: List of each value (multi-column selections use Tuple) in that row
        """
        table_rows = self.cursor.fetchone()
        return [row for row in table_rows]


if __name__ == '__main__':

    os.remove("TEST.db")  #remove existing datebase
    db = DatabaseInitialiser(db_address='TEST.db', player_datasets=['fifa13.csv', 'fifa14.csv', 'fifa15.csv', 'fifa16.csv',
                                                                    'fifa17.csv', 'fifa18.csv', 'fifa19.csv', 'fifa20.csv'], lineups_datasets=['LUO11119.csv'])

    db.establishConnection()
    db.setCursor()
    db.collectPlayers()
    db.collectClubs()
    db.collectLineups()
    db.assignClubIDs()
    db.createPlayersTable()
    db.createClubsTable()
    db.createClubMatchTable()
    db.createMatchesTable()
    db.createLineupTable()

    db.insertPlayers()
    db.insertClubs()
    db.insertMatches()

    db.closeDB()
