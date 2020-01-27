from appJar import gui
from model import *
from player import *
import re
import numpy as np


class View:
    """
    Forms the view in the MVC structure. Contains functionality to create a GUI.
    """

    def __init__(self, seasons: List):
        self._app = gui("Football Forecaster", useTtk=True)
        self._seasons: List = seasons
        self._selected_season: str
        self._controller = Controller(saved_model_dir="payOutModel.h5")
        self._clubs: List
        self._homeID: int
        self._awayID: int
        self._home_name: str
        self._away_name: str
        self._date: str

    def seasonButton(self) -> None:
        """
        Retrieves the user's season entry in the option box. Then, prepares date
        selection widget for next frame
        """
        self._selected_season: str = self._app.getOptionBox("Season")
        self.setClubs()  # Gets controller to retrieve clubs from that season
        self._app.nextFrame("Pages")
        season_years: List = re.findall("\d\d\d\d", self._selected_season)  # RegEx Search across the selected season to define the boundaries of date picker
        self._app.setDatePickerRange("dp", int(season_years[0]), int(season_years[1]))
        self._app.setDatePicker("dp")

    def dateButton(self) -> None:
        """
        Retrieves the user's date entry in the option box. Then, prepares club
        selection widget for next frame
        """
        self._date: str = self._app.getDatePicker("dp").strftime(r'%Y-%m-%d')  # Converts the users entered date to string
        self._app.nextFrame("Pages")
        club_names: List = [club[0] for club in self._clubs]
        self._app.addLabelOptionBox("Home Team", ["-- Home Team --"] + sorted(club_names))  # Teams in dropdown sorted alphabetically
        self._app.addLabelOptionBox("Away Team", ["-- Away Team --"] + sorted(club_names))

    def clubButton(self) -> None:
        """
        Retrieves the user's clubs entries in the option box. Then, prepares player
        selection widgets for next frame
        """
        self._home_name = self._app.getOptionBox("Home Team")
        self._away_name = self._app.getOptionBox("Away Team")
        if (self._home_name == None) or (self._away_name == None):  # If club dropdown left empty
            self._app.warningBox("club entry error", "Ensure both a Home Team and an Away Team has been entered!")
        elif self._home_name == self._away_name:  # duplicated club entry
            self._app.warningBox("club entry error", "Ensure the Home and Away Team are different!")
        else:
            self._app.removeOptionBox("Home Team")
            self._app.removeOptionBox("Away Team")
            self._app.nextFrame("Pages")
            self._homeID = [club[1] for club in self._clubs if club[0] == self._home_name][0]  # Finds the clubID of the selected club
            self._awayID = [club[1] for club in self._clubs if club[0] == self._away_name][0]
            home_team = [player[0] for player in self._controller.getPlayers(clubID=self._homeID)]  # List of players from the clubID
            away_team = [player[0] for player in self._controller.getPlayers(clubID=self._awayID)]
            for i in range(1, 12):
                self._app.addLabelOptionBox("Home Player {}".format(str(i)), ["-- Home Player --", "unlisted player"] + sorted(home_team), column=0)
                self._app.addLabelOptionBox("Away Player {}".format(str(i)), ["-- Away Player --", "unlisted player"] + sorted(away_team), column=2)

    def playerButton(self) -> None:
        """
        Retrieves the user's player entries in the option box. Then, passes the
        remaining information into the controller's instantiate method before displaying
        the prediction to the user.
        """
        home_players = [self._app.getOptionBox("Home Player {}".format(str(box_number))) for box_number in range(1, 12)]  # gets values from 11 player entry widgets
        away_players = [self._app.getOptionBox("Away Player {}".format(str(box_number))) for box_number in range(1, 12)]
        # Duplication flag is True when there is a duplicated player in either home or away
        duplication_flag = (len(set([player for player in home_players if player != "unlisted player"])) < len(
                                    [player for player in home_players if player != "unlisted player"]) or
                            len(set([player for player in away_players if player != "unlisted player"])) < len(
                                    [player for player in away_players if player != "unlisted player"]))

        not_enough_flag = (len(home_players) < 11 or len(away_players) < 11)  # True when too few players were entered

        if duplication_flag or not_enough_flag:
            self._app.warningBox("player entry error", "Please ensure that each player is selected and there is no duplication!")
        else:
            for i in range(1, 12):
                self._app.removeOptionBox("Home Player {}".format(i))
                self._app.removeOptionBox("Away Player {}".format(i))
            self._controller.instantiate(home_players=home_players, away_players=away_players,
                                         player_season=self._selected_season, home_teamID=self._homeID,
                                         away_teamID=self._awayID, current_date=self._date)  # Instantiates the hypothetical match with Match, Team and Player objects
            prediction_class = self._controller.getPrediction()  # Feature Vector is given to the Model and the result is received
            if prediction_class == 0:  # The prediction model outputs a 0 for a draw
                self._app.infoBox("prediction", "{} will draw with {}".format(self._home_name, self._away_name))
            elif prediction_class == 1:  # home win
                self._app.infoBox("prediction", "{} will beat {}".format(self._home_name, self._away_name))
            elif prediction_class == 2:  # away win
                self._app.infoBox("prediction", "{} will beat {}".format(self._away_name, self._home_name))
            self._app.stop()

    def setClubs(self) -> None:
        """
        Passes all entered clubs back to the controller
        :return:
        """
        self._clubs = self._controller.getClubs(season=self._selected_season)

    def addSchema(self) -> None:
        """
        Handles the full structure of the application, uses FrameStacks to allow
        multiple pages
        """
        self._app.setTtkTheme("black")  # Theme set to black
        self._app.startFrameStack("Pages")  # Uses Frame stack layout

        self._app.startFrame()
        self._app.addButton("Submit Season...", self.seasonButton)  # Creates button with function self.seasonButton
        self._app.addLabelOptionBox("Season", self._seasons)  # Option box created with a list of seasons
        self._app.stopFrame()

        self._app.startFrame()
        self._app.addButton("Submit Date...", self.dateButton)
        self._app.addDatePicker("dp")
        self._app.stopFrame()

        self._app.startFrame()
        self._app.addButton("Submit Clubs...", self.clubButton)
        self._app.stopFrame()

        self._app.startFrame()
        self._app.addButton("Submit Players...", self.playerButton)
        self._app.stopFrame()

        self._app.stopFrameStack()
        self._app.firstFrame("Pages")

    def startApp(self) -> None:
        """
        Starts the app. Loads all widgets added to the gui() object
        """
        self._app.go()


class Controller:
    def __init__(self, saved_model_dir=None):
        self.view: View
        self.database_selector = DatabaseSelector(db_address="DB071219.db")
        self.model: NeuralNet
        self.saved_model_dir: str = saved_model_dir
        self._feature_array: List
        self._prediction: int
        self.establishSQLConnection()

    def establishSQLConnection(self) -> None:
        """
        This method is called in the initialiser to enable the database connection
        """
        self.database_selector.establishConnection()
        self.database_selector.setCursor()

    def setUpModel(self) -> None:
        """
        Loads a pre-trained model and sets it as an attribute
        """
        self.model = NeuralNet()
        self.model.loadModel(model_path_dir=self.saved_model_dir)

    def makePrediction(self) -> None:
        """
        Feeds the feature array into the pre-trained model and sets the outcome
        to an attribute
        """
        self._prediction = self.model.predictOutcome(features=self._feature_array)[0]

    def getPrediction(self) -> int:
        return self._prediction

    def setUpView(self) -> None:
        """
        Called to create an instance of the View and start the application
        """
        self.view = View(seasons=self.getSeasons())
        self.view.addSchema()
        self.view.startApp()

    def getSeasons(self) -> List:
        """
        Calls the database model to collect every season held in the database
        """
        return self.database_selector.selectSeasons()

    def getClubs(self, season: str) -> List[Tuple,]:
        """
        Calls database model to collect every club within the selected season
        :param season: String of current season e.g. '2017/2018'
        :return: List of tuples in form (CLUB NAME, clubID)
        """
        return self.database_selector.selectClubNames(season=season)

    def getPlayers(self, clubID: int) -> List:
        """
        Calls database model to collect every player from that club
        :param clubID: Integer primary key
        :return: List of player names
        """
        return self.database_selector.selectPlayersFromClub(clubID=clubID)

    def instantiate(self, home_teamID: int, away_teamID: int, home_players: List[str], away_players: List[str],
                                                                player_season: str, current_date: str) -> None:
        """
        Handles manipulation of input data to form a feature vector compatible with the
        model, to make a prediction.
        :param home_teamID: Integer primary key
        :param away_teamID: Integer primary key
        :param home_players: List of selected home player name
        :param away_players: List of selected away player name
        :param player_season: String of current season
        :param current_date: String of current date in ISO 8601 format
        """
        match_obj = Match(matchID=None, score=None, match_date=current_date,
                          season=player_season)  # Match object instantiated wit matchID and score as None as the match hasn't taken place
        home_team = Team(teamID=home_teamID, team_date=current_date)
        away_team = Team(teamID=away_teamID, team_date=current_date)
        match_obj.addHomeTeam(home_team)  # Composes the Team object ot the Match object
        match_obj.addAwayTeam(away_team)
        home_player_data = [self.database_selector.selectPlayerData(player_name=player_name,
                                                                    player_season=player_season) for player_name in home_players]  # Gathers player data of each player in a List
        away_player_data = [self.database_selector.selectPlayerData(player_name=player_name,
                                                                    player_season=player_season) for player_name in away_players]
        assert len(home_player_data) == len(away_player_data)
        for h_player, a_player in zip(home_player_data, away_player_data):  # Simultaneous iteration across the equal length lists
            if len(h_player):  # If length is 0 where 'unlisted' is selected, the player is ignored
                home_player_id, home_player_name, home_name_long, home_position, home_rating, home_clubID, home_season = h_player[0]
                home_team.addPlayer(Player(playerID=home_player_id, name=home_player_name, name_long=home_name_long, clubID=home_clubID,
                                            position=home_position, rating=home_rating, season=home_season))  # Composes Player object to Team object
            if len(a_player):
                away_player_id, away_player_name, away_name_long, away_position, away_rating, away_clubID, away_season = a_player[0]
                away_team.addPlayer(Player(playerID=away_player_id, name=away_player_name, name_long=away_name_long,
                                clubID=away_clubID, position=away_position, rating=away_rating, season=away_season))
        for team in [home_team, away_team]:  # Iterates through each team object and calculates form and metrics
            team.setUpConnection()  # Database connection started
            team.calculateRecentForm()
            team.calculateRatingMetrics()
        match_obj.aggregateFeatures()  # Creates feature vector
        self._feature_array = np.array([match_obj.getFeatures()])
        self.setUpModel()  # Pre-trained model loaded
        self.makePrediction()  # Feature vector given and prediction is returned


if __name__ == '__main__':
    gui_controller = Controller()
    gui_controller.setUpView()
