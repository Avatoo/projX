#==============================================================================================================================
# Do not use this script for any serious purpose - it's just a benchmark of the performance of
# RSingleCore, RParallel, RcppSingleCore, RcppParallel
#==============================================================================================================================



library(tidyverse)
library(RSQLite)
library(Rcpp)
library(lubridate)
library(tictoc)
library(doSNOW)

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




# For each entry in columns home_player_1, ..., away_player_11, find the corresponding overall rating for each player in dfPlayerAttributes
# using the closest date to the match date
# Then append the df for overall rating to dfMatchEngland 




#==============================================================================================================================
# Start benchmark CSingleCore, CParallel, RSingleCore, RParallel
#==============================================================================================================================


# Run C using single core
runCSingleCore = function(dfMatchEngland, dfPlayerAttributes) {
  Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
  sourceCpp("D:/Development/Programming/FootballBet/code/mapMatchPlayerToRating.cpp")
  
  # tic()
  listOverallRating = {}
  arrayHomeAwayPlayerCol = which(grepl("player", names(dfMatchEngland)) == TRUE)
  j = 0
  for (i in arrayHomeAwayPlayerCol) {
    j = j + 1
    playerIdFromMatchDf = dfMatchEngland[, i]
    dateFromMatchDf = dfMatchEngland[, "date"]
    listOverallRating[[j]] = mapMatchPlayerToRating(playerIdFromMatchDf,
                                                    dateFromMatchDf, 
                                                    dfPlayerAttributes)
    names(listOverallRating)[j] = paste(names(dfMatchEngland)[i], "_rating", sep = "")
  }
  
  dfOverallRating = do.call(data.frame, listOverallRating)
  # toc()
  
  
}



# R function needed to run R in parallel
mapMatchPlayerToRatingR = function(playerIdFromMatchDf,
                                   dateFromMatchDf, 
                                   dfPlayerAttributes) {
  overallRating = rep(NA, length(playerIdFromMatchDf))
  for (i in 1:length(playerIdFromMatchDf)) {
    overallRating[i] = dfPlayerAttributes %>% 
      filter(player_api_id == playerIdFromMatchDf[i]) %>% 
      mutate(dateDif = date - dateFromMatchDf[i]) %>% 
      filter(dateDif == min(dateDif)) %>% 
      select(overall_rating) %>% 
      as.numeric(.)
  }
  return (overallRating)
}




# Run R single core
runRSingleCore = function(dfMatchEngland, dfPlayerAttributes) {
  # tic()
  arrayHomeAwayPlayerColName = names(dfMatchEngland)[grepl("player", names(dfMatchEngland))]
  arrayHomeAwayPlayerColName = arrayHomeAwayPlayerColName
  dfOutput = foreach (i = arrayHomeAwayPlayerColName, .combine = cbind, .packages = "tidyverse", .export = "mapMatchPlayerToRatingR") %do% {
    playerIdFromMatchDf = dfMatchEngland[, i]
    dateFromMatchDf = dfMatchEngland[, "date"]
    arrayOverallRating = mapMatchPlayerToRatingR(playerIdFromMatchDf, 
                                                 dateFromMatchDf, 
                                                 dfPlayerAttributes)
    dfOverallRating = data.frame(arrayOverallRating)
    names(dfOverallRating)[1] = i
    return (dfOverallRating)
  }
  # toc()
}





# parallel using R function
cl <- makeSOCKcluster(7)
registerDoSNOW(cl)

runRParallel = function(dfMatchEngland, dfPlayerAttributes) {
  # tic()
  arrayHomeAwayPlayerColName = names(dfMatchEngland)[grepl("player", names(dfMatchEngland))]
  arrayHomeAwayPlayerColName = arrayHomeAwayPlayerColName
  dfOutput = foreach (i = arrayHomeAwayPlayerColName, .combine = cbind, .packages = "tidyverse", .export = "mapMatchPlayerToRatingR") %dopar% {
    playerIdFromMatchDf = dfMatchEngland[, i]
    dateFromMatchDf = dfMatchEngland[, "date"]
    arrayOverallRating = mapMatchPlayerToRatingR(playerIdFromMatchDf, 
                                                 dateFromMatchDf, 
                                                 dfPlayerAttributes)
    dfOverallRating = data.frame(arrayOverallRating)
    names(dfOverallRating)[1] = i
    return (dfOverallRating)
  }
  # toc()
}





# Run C in Parallel
runCParallel = function(dfMatchEngland, dfPlayerAttributes) {
  # tic()
  arrayHomeAwayPlayerColName = names(dfMatchEngland)[grepl("player", names(dfMatchEngland))]
  arrayHomeAwayPlayerColName = arrayHomeAwayPlayerColName
  dfOutput = foreach (i = arrayHomeAwayPlayerColName, .combine = cbind, 
                      .packages = c("tidyverse", "Rcpp", "MapMatchPlayerToRating"), 
                      .noexport = c("mapMatchPlayerToRating")) %dopar% {
                        Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
                        
                        playerIdFromMatchDf = dfMatchEngland[, i]
                        dateFromMatchDf = dfMatchEngland[, "date"]
                        arrayOverallRating = mapMatchPlayerToRating(playerIdFromMatchDf, 
                                                                    dateFromMatchDf, 
                                                                    dfPlayerAttributes)
                        dfOverallRating = data.frame(arrayOverallRating)
                        names(dfOverallRating)[1] = i
                        return (dfOverallRating)
                      }
  # toc()
}









timing = microbenchmark(
  "runCParallel" = {
    arrayHomeAwayPlayerColName = names(dfMatchEngland)[grepl("player", names(dfMatchEngland))]
    arrayHomeAwayPlayerColName = arrayHomeAwayPlayerColName
    dfOutput = foreach (i = arrayHomeAwayPlayerColName, .combine = cbind, 
                        .packages = c("tidyverse", "Rcpp", "MapMatchPlayerToRating"), 
                        .noexport = c("mapMatchPlayerToRating")) %dopar% {
                          Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
                          
                          playerIdFromMatchDf = dfMatchEngland[, i]
                          dateFromMatchDf = dfMatchEngland[, "date"]
                          arrayOverallRating = mapMatchPlayerToRating(playerIdFromMatchDf, 
                                                                      dateFromMatchDf, 
                                                                      dfPlayerAttributes)
                          dfOverallRating = data.frame(arrayOverallRating)
                          names(dfOverallRating)[1] = i
                          return (dfOverallRating)
                        }
  }, 
  "runCSingleCore" = runCSingleCore(dfMatchEngland, dfPlayerAttributes), 
  "runRParallel" = runRParallel(dfMatchEngland, dfPlayerAttributes), 
  "runRSingleCore" = runRSingleCore(dfMatchEngland, dfPlayerAttributes), 
  times = 30)


