library(tidyverse)
library(RSQLite)
library(Rcpp)
library(lubridate)
library(tictoc)
library(doSNOW)
library(keras)

#==============================================================================================================================
# Read from SQL database
#==============================================================================================================================

# connect to sql database
connectToSQL <- dbConnect(drv = RSQLite::SQLite(), 
                          dbname = "D:/Development/Programming/FootballBet/data/player_stats/EuropeanSoccerDatabase.sqlite")

# list all tables
# exclude sqlite_sequence (contains table information)
tablesEuroPlayer <- dbListTables(connectToSQL) %>% 
  .[. != "sqlite_sequence"]

# Read in SQL tables to a list
listEuroPlayerSQL <- vector("list", length = length(tablesEuroPlayer))
for (i in seq(along = tablesEuroPlayer)) {
  listEuroPlayerSQL[[i]] <- dbGetQuery(conn = connectToSQL, statement=paste("SELECT * FROM '", tablesEuroPlayer[[i]], "'", sep = ""))
}
names(listEuroPlayerSQL) = tablesEuroPlayer
# Disconnect SQL database
dbDisconnect(conn = connectToSQL)


#==============================================================================================================================
# Extract player attributes info
#==============================================================================================================================
dfCountry = listEuroPlayerSQL[["Country"]]
dfLeague = listEuroPlayerSQL[["League"]]
dfMatch = listEuroPlayerSQL[["Match"]] %>% 
  mutate(date = ymd_hms(date))
dfPlayer = listEuroPlayerSQL[["Player"]]
dfPlayerAttributes = listEuroPlayerSQL[["Player_Attributes"]] %>% 
  mutate(date = ymd_hms(date))
dfTeam = listEuroPlayerSQL[["Team"]]
dfTeamAttributes = listEuroPlayerSQL[["Team_Attributes"]] %>% 
  mutate(date = ymd_hms(date))

# Clean duplicated rows in dfPlayerAttributes: if player_api_id and date are the same, keep only the first row
dfPlayerAttributes = dfPlayerAttributes %>% 
  arrange(player_api_id, desc(date), overall_rating) %>% 
  group_by(player_api_id, date) %>% 
  slice(1) %>% 
  ungroup()

# Clean duplicated rows in dfTeamAttributes: if team_api_id and date are the same, keep only the first row
dfTeamAttributes = dfTeamAttributes %>% 
  arrange(team_api_id, desc(date), buildUpPlaySpeed) %>% 
  group_by(team_api_id, date) %>% 
  slice(1) %>% 
  ungroup()


#==============================================================================================================================
# Analysis on premier league only for now
#==============================================================================================================================
# Focus on Premier league for now
dfTeamEngland = filter(dfTeam, 
                       team_short_name %in% c("MUN", "NEW", "ARS", "WBA", "SUN", "LIV", "WHU", "WIG", "AVL", "MCI", "EVE", "BLB", "MID", "TOT", "BOL", "STK", "HUL", "FUL", "CHE", 
                                              "POR", "BIR", "WOL", "BUR", "BLA", "SWA", "QPR", "NOR", "SOU", "REA", "CRY", "CAR", "LEI", "BOU", "WAT"))

dfTeamEnglandAttributes = filter(dfTeamAttributes, 
                                 team_api_id %in% dfTeamEngland[, "team_api_id"])

# Premier League id: 1729
# Select only matches of premier league
# Remove a few columns (they might be useful, but with them it's hard to display the whole dataframe)
dfMatchEngland = dplyr::filter(dfMatch, league_id == 1729) %>% 
  select(-goal, -shoton, -shotoff, -foulcommit, -card, -cross, -corner, -possession) %>% 
  select(-contains("away_player_X"), -contains("away_player_Y"), -contains("home_player_X"), -contains("home_player_Y"))




#==============================================================================================================================
# Data wrangling for past premier league matches

# For each entry in columns home_player_1, ..., away_player_11, find the corresponding overall rating for each player in dfPlayerAttributes
# using the closest date to the match date
cl <- makeSOCKcluster(7)
registerDoSNOW(cl)

# Need to install package "MapMatchPlayerToRating" in RPackage folder
arrayHomeAwayPlayerColName = names(dfMatchEngland)[grepl("player", names(dfMatchEngland))]
arrayHomeAwayPlayerColName = arrayHomeAwayPlayerColName
dfOverallRating = foreach (i = arrayHomeAwayPlayerColName, .combine = cbind, 
                    .packages = c("tidyverse", "Rcpp", "MapMatchPlayerToRating"), 
                    .noexport = c("mapMatchPlayerToRating")) %dopar% {
                      Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
                      
                      playerIdFromMatchDf = dfMatchEngland[, i]
                      dateFromMatchDf = dfMatchEngland[, "date"]
                      arrayOverallRating = mapMatchPlayerToRating(playerIdFromMatchDf, 
                                                                  dateFromMatchDf, 
                                                                  dfPlayerAttributes)
                      dfOverallRating = data.frame(arrayOverallRating)
                      names(dfOverallRating)[1] = paste(i, "_rating", sep = "")
                      return (dfOverallRating)
                    }

stopCluster(cl)



# check NAs in dfOverallRating
# Then append the df for overall rating to dfMatchEngland 
if (all(sapply(dfOverallRating, function(x) {sum(is.na(x))}) == 0)) {
  dfMatchEnglandWithRating = cbind(dfMatchEngland, dfOverallRating)
} else {
  stop("There are NAs in dfOverallRating. Clean NAs before continue - Neural Nets cannot accept NA values")
}






#==============================================================================================================================
# Data wrangling for premier league team attributes

# Check NAs in dfTeamAttributes
sapply(dfTeamAttributes, function(x) {sum(is.na(x))})
# Column buildUpPlayDribbling has lots of entries = NA
# No other column has NAs
# Exclude column buildUpPlayDribbling for further analysis
# Also convert character columns to factors
dfTeamAttributesAfterClean = cbind(dfTeamAttributes[sapply(dfTeamAttributes, class) != "character"], 
                                   sapply(dfTeamAttributes[sapply(dfTeamAttributes, class) == "character"], as.factor)) %>% 
  select(-buildUpPlayDribbling)



 # For each match in dfMatchEngland, find the score of one of the team (can be home or away team, depending on input parameters) from dfTeamAttributesAfterClean
 #   - Score can be any numeric columns from dfTeamAttributesAfterClean (apart from id, team_fifa_api_id, team_api_id, date)
 #   - Score for the team will be chosen based on the date which is closest to the match date

Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
sourceCpp("D:/Development/Programming/Git/projX/code/mapTeamInMatchDfToNumericScore.cpp")

# Initiate variables to store output from the Rcpp function "mapTeamInMatchDfToNumericScore"
listTeamScoreOutput = {}
arrayHomeAway = c("home", "away")
# An array storing colname of dfTeamAttributesAfterClan (apart from these 4 columns "id", "team_fifa_api_id", "team_api_id", "date")
arrayNumericColName = names(dfTeamAttributesAfterClean)[-(1:4)]
k = 0
for (j in arrayHomeAway) {
  for (i in 1:length(arrayNumericColName)) {
    k = k + 1
    team_api_id_from_match_df = dfMatchEnglandWithRating[, paste(j, "_team_api_id", sep = "")]
    dateFromMatchDf = dfMatchEnglandWithRating[, "date"]
    team_api_id_from_team_df = dfTeamAttributesAfterClean[, "team_api_id"]
    dateFromTeamDf = dfTeamAttributesAfterClean[, "date"]
    scoreFromTeamDf = dfTeamAttributesAfterClean[, arrayNumericColName[i]]
    
    scoreOutput = mapTeamInMatchDfToNumericScore(team_api_id_from_match_df, 
                                                 dateFromMatchDf, 
                                                 team_api_id_from_team_df, 
                                                 dateFromTeamDf, 
                                                 scoreFromTeamDf)
    listTeamScoreOutput[[k]] = scoreOutput
    names(listTeamScoreOutput)[k] = paste(j, "_", arrayNumericColName[i], sep = "")
  }
}
dfTeamScore = do.call(data.frame, listTeamScoreOutput)

# Append dfTeamScore back to dfMatchEnglandWithRating
dfMatchEnglandWithRatingTeamScore = cbind(dfMatchEnglandWithRating, dfTeamScore)

