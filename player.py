from typing import List, Tuple, Union
from database import *
import numpy as np
import re


class Match:
    """
    This class encapsulates all the information of a match. Composes 2 Team
    objects (Home and Away). Additionally, feature vectors for the prediction model
    are produced.
    """
    def __init__(self, matchID : int, score : str, match_date : str, season : str):
        """
        :param matchID: Integer Primary Key
        :param score: String of match result in form: 'home_score - away_score'
        :param match_date: String of date in ISO 8601 format
        :param season: String of the current season e.g. '2017/2018'
        """
        self._home_team : Team
        self._away_team : Team
        self._matchID   : int = matchID
        self._score     : str = score
        self._date      : str = match_date
        self._season    : str = season
        self._features  : List


    def addHomeTeam(self, team : 'Team') -> None:
        """
        Composes the home Team object
        :param team: Team object of Home team
        """
        self._home_team = team

    def addAwayTeam(self, team) -> None:
        """
        Composes the away Team object
        :param team: Team object of away team
        """
        self._away_team = team

    def getHomeTeam(self) -> 'Team':
        """
        :return: composed home Team object
        """
        return self._home_team

    def getAwayTeam(self) -> 'Team':
        """
        :return: composed away Team object
        """
        return self._away_team

    def getMatchID(self) -> int:
        return self._matchID

    def aggregateFeatures(self) -> None:
        """
        Creates a List of the 10 metrics which will be converted to a NumPy array
        later on for the model
        """
        features: List = []  #List to represent feature vector
        for team in [self._home_team, self._away_team]:
            averages : List = team.getRatingMetrics()  #gets the rating metrics attribute
            recent_form : float = team.getRecentForm()  #gets the recent form attribute
            for avg in averages:
                features.append(avg)
            features.append(recent_form)
        if self._score != None:  #only triggered during training, as result is available
            home_score = int(re.search('\A\d', self._score).group(0))  #RegEx search to find home score
            away_score = int(re.search('\d\Z', self._score).group(0))  #RegEx search to find away score
            if home_score == away_score:
                features.append(0)  #0 is the numerical placeholder of a draw
            elif home_score > away_score:
                features.append(1)  #1 is the numerical placeholder of a home win
            elif home_score < away_score:
                features.append(2)  #2 is the numerical placeholder of a away win
        self._features = features

    def getFeatures(self) -> List:
        """
        :return: List length [10,] containing all 10 metrics
        """
        return self._features


class Team:
    """
    The Team class encapsulates all features of a match and composes 11 players and
    contains information about the club.
    Also, recent form and team metrics are calculated. Additonally, this is the
    only class within the Match-Team-Player classes with a connection to the database.
    """
    def __init__(self, teamID : int, team_date : str):
        self._players   : List = []
        self._teamID    : int = teamID
        self._date  : str = team_date
        self._positions : Dict[str : List[str,]] = {'GOALKEEPER' : ['GK'],
                                        'DEFENCE' : ['RB', 'RWB', 'CB', 'LB', 'LWB'],
                                        'MIDFIELD' : ['CDM', 'LM', 'CM', 'RM', 'CAM'],
                                        'FORWARD' : ['LW', 'CF', 'RW', 'ST']}
        self._database_selector : DatabaseSelector
        self._average_ratings: List = []
        self._recent_form : int = 0


    def getPlayers(self) -> List:
        """
        :return: List of Player Objects
        """
        return self._players

    def setUpConnection(self) -> None:
        """
        Instantiates a DatabaseSelector object and establishes a connection. Enables
        recent form to be calculated by calculateRecentForm
        """
        self._database_selector = DatabaseSelector('DB071219.db')
        self._database_selector.establishConnection()
        self._database_selector.setCursor()

    def addPlayer(self, player : 'Player') -> None:
        """
        Composes another player to the object
        :param player: Player object
        """
        self._players.append(player)

    def getTeamID(self) -> int:
        """
        :return: Integer primary Key
        """
        return self._teamID

    def calculateRatingMetrics(self) -> None:
        """
        This method averages the player ratings across the field of play. Creates a
        List of each numerical metric.
        """
        lineup = np.array([player.getRating() for player in self._players])  #NumPy array of each rating in the entire line-up
        defenders = np.array([player.getRating() for player in self._players if (player.getPosition() in self._positions['DEFENCE']) or
                                        (player.getPosition() in self._positions['GOALKEEPER'])])  #Subset of lineup, based on position
        midfielders = np.array([player.getRating() for player in self._players if player.getPosition() in self._positions['MIDFIELD']])
        forwards = np.array([player.getRating() for player in self._players if player.getPosition() in self._positions['FORWARD']])
        averages = []
        for player_group in [lineup, defenders, midfielders, forwards]:
            if not len(player_group):  #When the length of the array is empty, 0 is added as a placeholder, this will take the average rating later
                averages.append(0)
                continue
            averages.append(np.mean(player_group))  #The mean is taken of each NumPy array
        self._average_ratings = averages

    def calculateRecentForm(self) -> None:
        """
        This method uses values held within the database to calculate the points per
        game over the last month.
        """
        recent_matches: List[Tuple[int, str, int]] = self._database_selector.getRecentMatches(match_date=self._date, clubID=self._teamID)
        match_count = len(recent_matches)
        if not match_count:  #if beginning of the season, recent form is set to 0 and subroutine is exited
            self._recent_form = 0.0
            return None
        total_points : float = 0.0
        for r_match in recent_matches:
            home_score : int = int(re.search('\A\d', r_match[1]).group(0))  #RegEx to find home score of each recent game
            away_score : int = int(re.search('\d\Z', r_match[1]).group(0))
            if home_score == away_score:
                total_points += 1.0  #1 point awarded for a win
            elif home_score > away_score and r_match[0] == self._teamID:
                total_points += 3.0  #3 points added if the team wins at home
            elif away_score > home_score and r_match[2] == self._teamID:
                total_points += 3.0  #3 points added if the team wins away
        try:
            self._recent_form : float = (total_points / match_count)  #points-per-game is calculated
        except ZeroDivisionError:
            self._recent_form : float = 0.0  #ZeroDiv occurs when there are no recent matches eg

    def getRatingMetrics(self) -> List:
        """
        :return: List of calculated averages
        """
        return self._average_ratings

    def getRecentForm(self) -> float:
        """
        :return: float value of points per game over the previous month
        """
        return self._recent_form

class Player:
    """
    Encapsulates all attributes related to a single player. Only accessor methods.
    """
    def __init__(self, playerID, name, name_long, position, rating, clubID, season):
        self._playerID  : int = playerID
        self._name      : str = name
        self._name_long : str = name_long
        self._position  : str = position
        self._rating    : int = rating
        self._clubID    : int = clubID
        self._season    : str = season

    def getPlayerID(self) -> int:
        return self._playerID

    def getName(self) -> str:
        return self._name

    def getNameLong(self) -> str:
        return self._name_long

    def getPosition(self) -> str:
        return self._position

    def getRating(self) -> int:
        return self._rating

    def getClubID(self) -> int:
        return self._clubID

    def getSeason(self) -> str:
        return self._season


def buildSets(shuffle : bool) -> List[List,]:
    """
    assembles a dataset of a collection of feature vectors from every Premier League
    match held in the table.
    :param shuffle: Boolean
    :return: List of feature vectors of every match in the database
    """
    training_testing_selector = DatabaseSelector(db_address='DB071219.db')
    training_testing_selector.establishConnection()
    training_testing_selector.setCursor()
    database_matches : List[Tuple,] = training_testing_selector.getMatches()
    dataset : List[List,] = []
    for match in database_matches:  #iterates through each stored match in database
        match_obj = Match(matchID=match[0], score=match[4], match_date=match[6], season=match[1])   #Instantiates the Match object
        home_team = Team(teamID=match[2], team_date=match[6])  #Instantiates each Team object
        away_team = Team(teamID=match[3], team_date=match[6])
        for team_obj in [home_team, away_team]:
            team_obj.setUpConnection()  #Links Team object to database
            team_obj.calculateRecentForm()  #Evaulates each rating metrics for each team
        lineups: Tuple[List, List] = training_testing_selector.selectLineup(lineupid=match[5])
        home_ids, away_ids = lineups
        for h_id, a_id in zip(home_ids, away_ids):  #Simultaneously iterates across each list of IDs
            if h_id != 0:  #If a player ID is 0, it is ignored as it represents an unknown player
                home_player: List = training_testing_selector.selectPlayer(h_id)
                home_player_object = Player(*home_player)  #intantiates each Player object
                home_team.addPlayer(home_player_object)  #Composes each player to the Match
            if a_id != 0:
                away_player: list = training_testing_selector.selectPlayer(a_id)
                away_player_object = Player(*away_player)
                away_team.addPlayer(away_player_object)

        home_team.calculateRatingMetrics()
        away_team.calculateRatingMetrics()

        match_obj.addHomeTeam(home_team)
        match_obj.addAwayTeam(away_team)

        match_obj.aggregateFeatures()
        dataset.append(match_obj.getFeatures())  #adds the feature vector
    if shuffle:
        np.random.shuffle(dataset)  #Shuffles the order of the dataset to prevent the training and testing having contiguous match dates
    return dataset

def splitTrainingTesting(dataset : List, partition_ratio : float) -> List[List,]:
    """
    creates a partition for training and testing sets.
    :param dataset: List of feature vectors
    :param partition_ratio: proportion of training : testing (e.g. 0.7 = 70% training)
    :return: List containing training and testing sets
    """
    training_size : float = round(len(dataset) * partition_ratio)
    training_set  : List = dataset[:training_size]
    testing_set   : List = dataset[training_size:]
    return [training_set, testing_set]
