from typing import *
import bs4
from bs4 import BeautifulSoup
import requests
import re
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.common.exceptions import *
import urllib3
import time


class Scraper:
    """
    Scraper is a class which encapsulates all data collection methods used to
    web-scrape player and match data.
    """
    def __init__(self, root_url : str, csv_location : str):
        self.root_url : str = root_url
        self.soup = None
        self.current_page = None
        self.csv_location : str = csv_location

    def setSoup(self, verification : bool = True) -> None:
        """
        Fetches the HTML of the current webpage and assigns it to a BeautifulSoup
        object for parsing. This is held as an attribute.
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        try:
            page = requests.get(self.root_url, verify=verification)
            assert page.status_code == 200  #ensures status code: OK
            self.soup = BeautifulSoup(page.text, "lxml")  #sets BeautifulSoup as HTML parser
            self.current_page = page
        except requests.exceptions.SSLError as e:  #connection not possible
            print("Verification error :", e)
            self.setSoup(verification=False)  #Retry without SSL verification
        except requests.ConnectionError as e:
            print("Could not connect, likely bad WiFi connection", e)
            exit(1)

    def findTable(self, all_tags, table_class : Dict) -> bs4.element.NavigableString:
        """
        :param all_tags: The full HTML listing of the web page
        :param table_class: The HTML class of the table tag
        :return: HTML of the table

        This is used to locate tables in both child classes hence it appearing in
        the parent class to encourage code-reuse
        """
        try:
            return all_tags.find('table', table_class).find('tbody')
        except AttributeError as e:
            print("Can't find table, incorrect page HTML format.", e)
            exit(1)



class PlayerScraper(Scraper):
    """
    Encapsulates all methods to scrape all Premier League FIFA players off sofifa.com
    """
    def __init__(self, root_url : str, csv_location : str, season : str):
        super().__init__(root_url, csv_location)
        self._extracted_players : {str : [str, str, str, str]} = {}
        self._currentID : int = 1
        assert re.match(r"\d+/\d+", season), "Season name must be in form: digits/digits"
        self._season : str = season

    def changePageURL(self) -> None:
        """
        Adjusts the content of the navigable URL through RegEx to go to the next page of players.
        """
        offset = int(re.search(r"\d+\Z", self.root_url).group(0)) + 60
        self.root_url = re.sub(r"\d+\Z", "", self.root_url) + str(offset)

    def terminateRecursion(self) -> bool:
        """
        When sofifa reaches the last player page, it returns to the root URL - /players,
        recursion must end at this point
        :return: False to terminate recursion in extractPlayers method
        """
        if re.match(r"https://sofifa.com/players\Z", self.current_page.url):  #\Z : end of string
            return True
        return False

    def extractPlayers(self) -> bool:
        """
        Recursively moves across sofifa webpages containing player data and performs
        necessary HTML parsing to extract players and their attributes
        """
        self.setSoup()
        if self.terminateRecursion():  # base-case for recursion
            return True
        table_tags = self.findTable(all_tags=self.soup, table_class={'class': 'table table-hover persist-area'})
        for player in table_tags.findAll('tr'):  #Iterates through each table row tag
            current_player = []
            for attribute in player.findAll('td'):  #Iterates through each table column
                try:
                    if attribute['class'] == ['col-name'] and attribute.find('a', {'class': 'nowrap'}):  #If the current tag has class - column name - AND an <a> tag exists
                        name_tag = attribute.find('a', {'class': 'nowrap'})  #Finds the <a> tag which holds the player name
                        position_tag = attribute.find('span', {'class': 'pos'})  #Finds the <span> tag which holds position
                        current_player.append(name_tag.get_text())  #Appends the extracted text data from the tag for the player SHORT name (e.g. T.Cairney)
                        current_player.append(name_tag.get('title'))  #Appends the text data relating to the player LONG name (e.g. Tom Cairney)
                        current_player.append(position_tag.get_text())  #Appends the extracted text data relating to position
                    elif attribute['class'] == ["col", "col-oa", "col-sort"]:  # If tag has the classes in the list, it relates the the rating tag
                        overall_rating_tag = attribute.find('span', {'class': 'bp3-tag'})  #Finds the tag which holds the overall player rating
                        current_player.append(overall_rating_tag.get_text())
                    elif attribute['class'] == ['col-name']:  #If the tag only has class - column name - it relates to the club tag
                        club_tag = attribute.a  #The <a> tag is selected
                        current_player.append(club_tag.get_text())
                        continue #continue to next iteration as relevant tags have been collected
                except AttributeError as e:
                    print("player not received", e)
                    continue
            self._extracted_players[current_player[0]] = current_player[1:]  #Stores the player in a Dict with KEY : VALUE, Player name : List of attributes
        self.changePageURL()  #Offsets the URL to move to next page
        self.extractPlayers()  #Recursive tail-call

    def writePlayerCSV(self) -> None:
        """
        Writes all webscraped sofifa player data into csv files, formatted with RegEx
        """
        with open(self.csv_location, "w", encoding="utf-16") as file:
            for extracted_player in self._extracted_players:
                player_name = extracted_player
                print(self._extracted_players[extracted_player])
                assert len(self._extracted_players[extracted_player]) == 4  #ensures length is 5 to confirm the values can be unpacked
                player_long_name, player_position, player_rating, player_club = self._extracted_players[extracted_player]
                csv_format = re.compile(
                    player_name + "," + player_long_name + "," + player_position + "," + player_rating + "," + player_club + "," + self._season + "\n")
                file.write(csv_format.pattern)  #Writes the compiled RegEx pattern with the values inserted


class ResultScraper(Scraper):
    """
    Encapsulates all methods required to webscrape match data on soccerway website
    """
    def __init__(self, csv_location : str, season_url : str, webdriver_path : str):
        super().__init__(root_url='', csv_location=csv_location)
        self._season_url : str = season_url
        self._webdriver : None = None
        self._extracted_matches : Dict = {}
        self._line_up_tables : List = []
        self._extracted_links = []
        self._webdriver_path : str = webdriver_path

    def offsetSeasonURL(self) -> bool:
        """
        Increases the year on the current season, therefore the URL will link to the
        page of the next season
        :return: True if the current season is the current season
        """
        season_number = re.search(r"/\d+/", self._season_url).group(0)
        season_value = re.search(r"\d+", season_number).group(0)
        new_url = re.sub(r"/\d+/", "", self._season_url)
        new_years = str(int(season_value[:4]) + 1) + str(int(season_value[4:]) + 1)
        if new_years == '20202021':  #Ends at current season, will end recursion
            return True
        self._season_url = "{}/{}/".format(new_url, new_years)
        return False

    def setWebdriver(self) -> None:
        """
        Creates a webdriver to operate a web browser. Also deals with dismissing
        GDPR data usage agreements to enable access to the page
        """
        try:
            browser = webdriver.Chrome(executable_path=self._webdriver_path)
            browser.get(self._season_url)  #redirects webdriver to page
            WebDriverWait(browser, 25).until(expected_conditions.presence_of_element_located((
                    By.XPATH, r'//*[@id="qcCmpButtons"]/button[2]')))  #Pauses all actions until the privacy warning appears
            browser.maximize_window()  #webdriver full screen mode
            time.sleep(2)  #all processes pause to allow the button to initialise
            gdpr_button = browser.find_element_by_xpath(r'//*[@id="qcCmpButtons"]/button[2]')  #the button is located by xPath
            browser.execute_script("arguments[0].click();", gdpr_button)  #executes JavaScript to click button
            time.sleep(2)  #all processes paused to allow the rest of the page to initialise
            self._webdriver = browser
        except WebDriverException as e:  #if webdriver isn't found
            print("Check webdriver path and URL", e)
            exit(1)

    def findButton(self) -> object:
        """
        Locates the xPath of the button which trigger the table to change to the
        previous match page. The WebDriver will wait a maximum of 25 seconds to wait
        for the button to be reachable on the page.
        :return: Selenium object relating to the button
        """
        WebDriverWait(self._webdriver, 25).until(expected_conditions.presence_of_element_located((
                        By.XPATH, r'//*[@id="page_competition_1_block_competition_matches_summary_5_previous"]')))
        return self._webdriver.find_element_by_xpath(r'//*[@id="page_competition_1_block_competition_matches_summary_5_previous"]')

    def findResultsTable(self) -> object:
        """
        Locates the results table
        :return: selenium element object
        :return: Selenium object relating to the entire results table
        """
        WebDriverWait(self._webdriver, 25).until(expected_conditions.presence_of_element_located((
                    By.XPATH, r'//*[@id="page_competition_1_block_competition_matches_summary_5"]/div[2]/table')))
        return self._webdriver.find_element_by_xpath(r'//*[@id="page_competition_1_block_competition_matches_summary_5"]/div[2]/table')


    def operateResultsTable(self) -> bool:
        """
        Recursively moves through each season and iterates across each match played
        in the season. Each href tag containing the URL of the match details is
        stored for later lineup gathering.
        """
        cached_match_tags: None = None  #Used to hold the tags of the last iteration to catch
        try:
            for i in range(38):
                button = self.findButton()
                results_table: object = self.findResultsTable()
                time.sleep(2)
                soup = BeautifulSoup(results_table.get_attribute('innerHTML'), 'lxml')
                match_tags = soup.findAll('td', {'class' : 'score-time score'})
                if cached_match_tags == match_tags:
                    break  #breaks when the table page repeats, therefore on last page
                cached_match_tags = match_tags
                for match in match_tags:
                    self._extracted_links.append(match.a.get('href'))
                self._webdriver.execute_script("arguments[0].click();", button)
                time.sleep(3)
        except WebDriverException as e:
            print("webdriver error:", e)

        self._webdriver.close()
        if self.offsetSeasonURL():  #base-case where the final season is reached, or offsets URL for next recursion
            return True
        else:
            self.setWebdriver()  #updates Webdriver details, root_url altered by offsetSeasonURL
            self.operateResultsTable()  #recursive tail-call

    def setLineUpTables(self):
        """
        Locates and saves the navigable tags of each lineup box
        """
        try:
            content_box = self.soup.find('div', {'class': 'combined-lineups-container'})  #Large container with both home and away
            if content_box == None:
                raise Exception
            containers = self.collectContainer(content_box)  #splits home/away containers
            self._line_up_tables = [(lambda container_tags: self.findTable(all_tags=container_tags, table_class={'class': 'playerstats lineups table'}))
                                    (container_tags) for container_tags in containers]  #locates home/away tables by applying .findTable on each container
        except Exception as e:  #If the Line-up table can't be located the page is ignored
            print(e)
            return True  #Returns True to let the calling method know that no data was located

    def collectContainer(self, content : bs4.element.NavigableString) -> List:
        """
        Utilizes lambda function to find the div tag held in each container, use of
        list comprehension to call over the right and left containers. This is used both
        in collectClubsResult and setLineUpTables, to encourage polymorphism this is
        its own method.
        :param content: BeautifulSoup navigable page tags
        :return: List containing the navigable tags in each box (right, left)
        """
        return [(lambda container_location: content.find('div', container_location))(container_location)
                      for container_location in [{'class': 'container left'}, {'class': 'container right'}]]

    def collectClubsResult(self) -> List:
        """
        Searches each match page for information about the match. Uses BeautifulSoup
        to parse HTML of each element.
        :return: List containing [CLUB, CLUB, SCORE, DATE]
        """
        match_info_box = self.soup.find('div', {'class' : 'block clearfix block_match_info-wrapper'})  #Gets tags relating to the info box
        scoreline_box = match_info_box.find('div', {'class' : 'clearfix'})
        clubs = self.collectContainer(scoreline_box)  #Gets content of right and left containers

        club_names = [club.a.get_text() for club in clubs]  #Gets each name out of each <a> tag

        scorebox = scoreline_box.find('div', {'class', 'container middle'})
        score_format = re.search(r"\d\s-\s\d", scorebox.h3.get_text()).group(0)  #Uses RegEx search to extract the scoore string

        club_names.append(score_format)

        date_tags = match_info_box.find('div', {'class' : 'details clearfix'}).dl
        date_string = date_tags.findAll('dd')[1].get_text()  #date in second instance of <dd> tag

        club_names.append(date_string)

        return club_names

    def extractLineup(self) -> None:
        """
        Iterates through each hyperlink found across of the match pages. Requests data
        from each match page to extract the lineup. Places scraped data in a
        dictionary called extracted_matches with all match data
        """
        for link in self._extracted_links:
            self.root_url = "https://int.soccerway.com{}".format(link)
            self.setSoup()
            if self.setLineUpTables():  #any error during locating the Lineup, the page is assumed to be invalid
                continue
            tables = self._line_up_tables
            current_match = []

            for team in tables:
                current_team = []
                for starting_player in team.findAll('tr')[:11]:  #Gets first 11 instances of <tr> table row tag
                    for player_detail in starting_player.findAll('td'):  #Iterates through each Player attribute
                        try:
                            if player_detail['class'] == ['player', 'large-link']:
                                player_name = player_detail.a.get_text()  #Extracts player name from <a> tag
                                current_team.append(player_name)
                        except KeyError as e:
                            print("player detail not found", e)
                            continue
                current_match.append(current_team)

            match_info = tuple(self.collectClubsResult())  #converts List to tuple of match info, to be a Dict Key
            self._extracted_matches[match_info] = current_match  #Stores in Dict {Tuple(MATCH INFO) : List of player names}

    def writeLineupCSV(self) -> None:
        """
        Iterates through all extracted match data and writes to a CSV file
        """
        with open(self.csv_location, "w", encoding="utf-16") as lineupCSV:
            for extracted_match in self._extracted_matches:
                home_team, away_team, score, date = extracted_match
                csv_format = home_team + "," + away_team + "," + score + "," + date + ","
                for team in self._extracted_matches[extracted_match]:
                    csv_format += ",".join(team)
                    if self._extracted_matches[extracted_match].index(team) == 0:
                        csv_format += ','
                csv_format += '\n'
                formatted = re.compile(csv_format)  #The pattern is generated
                lineupCSV.write(formatted.pattern)


def createPlayersDatasets(season_urls : List[Tuple[str, str, str]]) -> None:
    """
    Subroutine to instantiate and call methods to navigate the sofifa website and
    scrape player data
    :param season_urls: List containing tuples in the form:
    (root_url, csv_location, season)
    """
    scrapers = []
    for url in season_urls:
        scrapers.append(PlayerScraper(root_url=url[0], csv_location=url[1], season=url[2]))
    for season_scraper in scrapers:
        season_scraper.setSoup()
        season_scraper.extractPlayers()
        season_scraper.writePlayerCSV()


def createResultsDataset(csv_location : str, season_url : str, webdriver_path : str) -> None:
    """
    Subroutine to instantiate and call methods to navigate and scrape lineups
    :param csv_location: String of full path
    :param season_url: String of the URL of the starting season lineups will be scraped
    from
    :param webdriver_path: String of full path of Chrome WebDriver
    """
    result_scraper = ResultScraper(csv_location=csv_location,
                                   season_url=season_url,
                                   webdriver_path=webdriver_path)
    result_scraper.setWebdriver()
    result_scraper.operateResultsTable()
    result_scraper.extractLineup()
    result_scraper.writeLineupCSV()

if __name__ == '__main__':
    createPlayersDatasets([('https://sofifa.com/players?type=all&lg%5B0%5D=13&v=14&e=157760&set=true&offset=0', 'VIDEOFIFA14.csv', '2013/2014')])
    createResultsDataset(csv_location='VIDEOTEST.csv',
                         season_url='https://int.soccerway.com/national/england/premier-league/20192020/',
                         webdriver_path=r'C:/Users/Liam/Documents/chromedriver_311019/chromedriver.exe')
'''
if __name__ == '__main__':
    createPlayersDatasets([('https://sofifa.com/players?type=all&lg%5B0%5D=13&v=14&e=157760&set=true&offset=0', 'fifa14.csv', '2013/2014'),
                           ('https://sofifa.com/players?type=all&lg%5B0%5D=13&v=15&e=158116&set=true&offset=0', 'fifa15.csv', '2014/2015'),
                           ('https://sofifa.com/players?type=all&lg%5B0%5D=13&v=16&e=158494&set=true&offset=0', 'fifa16.csv', '2015/2016'),
                           ('https://sofifa.com/players?type=all&lg%5B0%5D=13&v=17&e=158857&set=true&offset=0', 'fifa17.csv', '2016/2017'),
                           ('https://sofifa.com/players?type=all&lg%5B0%5D=13&v=18&e=159214&set=true&offset=0', 'fifa18.csv', '2017/2018'),
                           ('https://sofifa.com/players?type=all&lg%5B0%5D=13&v=19&e=157340&set=true&offset=0', 'fifa19.csv', '2018/2019'),
                           ('https://sofifa.com/players?type=all&lg%5B0%5D=13&v=13&e=157340&set=true&offset=0', 'fifa20.csv', '2019/2020')])


    createResultsDataset(csv_location='test.csv',
                         season_url='https://int.soccerway.com/national/england/premier-league/20192020/',
                         webdriver_path=r'C:/Users/Liam/Documents/chromedriver_311019/chromedriver.exe')
'''

