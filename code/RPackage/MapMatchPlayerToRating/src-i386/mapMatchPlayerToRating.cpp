#include <Rcpp.h>
#include <iostream>
using namespace Rcpp;
using namespace std;


// For a given column in dfMatchEngland, e.g. home_player_1, find the correspnding overall rating for all of the players
// Overall rating will be based on the rating at the date such that it's the closest to the match date
// Input: 
//   - playerIdFromMatchDf: columns home_player_1, ..., away_player_11 in dfMatchEngland
//   - dateFromMatchDf: column date in dfMatchEngland
//   - dfPlayerAttributes: dfPlayerAttributes
// [[Rcpp::export]]
std::vector<int> mapMatchPlayerToRating(NumericVector playerIdFromMatchDf, 
                            NumericVector dateFromMatchDf, 
                            DataFrame dfPlayerAttributes) {
  
  // Vectors from dfPlayerAttributes
  std::vector<int> playerIdFromPlayerDf = as<std::vector<int>> (dfPlayerAttributes["player_api_id"]);
  std::vector<int> dateFromPlayerDf = as<std::vector<int>> (dfPlayerAttributes["date"]);
  std::vector<int> overallRating = as<std::vector<int>> (dfPlayerAttributes["overall_rating"]);
  
  // Output vector
  std::vector<int> overallRatingOutput = std::vector<int>();
  
  // Loop over i in playerIdFromMatchDf
  for (int i = 0; i < playerIdFromMatchDf.size(); ++i) {
    int playerId = playerIdFromMatchDf[i];
    
    // Find the indices of playerIdFromPlayerDf == playerId for i-th iteration, call this array rowNumber
    std::vector<int> rowNumber = std::vector<int>();
    for (int j = 0; j < (int) playerIdFromPlayerDf.size(); ++j) {
      if (playerIdFromPlayerDf[j] == playerId) {
        rowNumber.push_back(j);
      }
    }

    // From the indicies, extract corresponding dateFromPlayerDf and overall_rating
    std::vector<int> dateSubRange = std::vector<int>();
    std::vector<int> overallRatingSubRange = std::vector<int>();
    for (auto j = rowNumber.begin(); j != rowNumber.end(); ++j) {
      dateSubRange.push_back(dateFromPlayerDf[*j]);
      overallRatingSubRange.push_back(overallRating[*j]);
    }

    // From dateSubRange, find the date which is the closest to the match date, call it dateClosest
    // Likewise find the overall rating for this play at dateClosest
    int dateDif;
    int dateDifMin = INT_MAX;
    int dateClosest;
    int overallRatingThisDate;
    for (int j = 0; j < (int) dateSubRange.size(); ++j) {
      dateDif = abs(dateSubRange[j] - dateFromMatchDf[i]);
      if (dateDif <= dateDifMin) {
        dateDifMin = dateDif;
        dateClosest = dateSubRange[j];
        overallRatingThisDate = overallRatingSubRange[j];
      }
    }
    
    // Save the rating overallRatingThisDate to an array, which we will return after the loop finishes
    overallRatingOutput.push_back(overallRatingThisDate);
  }
  
  return (overallRatingOutput);
}


