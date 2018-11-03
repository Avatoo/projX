#include <Rcpp.h>
#include <iostream>
using namespace Rcpp;
using namespace std;


// For each match in dfMatchEngland, find the score of one of the team (can be home or away team, depending on input parameters) from dfTeamAttributesAfterClean
//   - Score can be any numeric columns from dfTeamAttributesAfterClean (apart from id, team_fifa_api_id, team_api_id, date)
//   - Score for the team will be chosen based on the date which is closest to the match date
// Input:
//     - team_api_id_from_match_df: column "team_api_id" from df dfMatchEnglandWithRating
//     - dateFromMatchDf: column "date" from df dfMatchEnglandWithRating
//     - team_api_id_from_team_df: column "team_api_id" from df dfTeamAttributesAfterClean
//     - dateFromTeamDf: column "date" from df dfTeamAttributesAfterClean
//     - scoreFromTeamDf: a column indicating score from df dfTeamAttributesAfterClean. This column has to be numeric, e.g. buildUpPlaySpeed, defenceTeamWidth

// [[Rcpp::export]]
std::vector<int> mapTeamInMatchDfToNumericScore(NumericVector team_api_id_from_match_df, 
                                                NumericVector dateFromMatchDf, 
                                                NumericVector team_api_id_from_team_df, 
                                                NumericVector dateFromTeamDf, 
                                                NumericVector scoreFromTeamDf) {
  // Output vector
  std::vector<int> scoreOutput = std::vector<int> ();
  
  // Iterate over each team in the match df
  for (int i = 0; i < (int) team_api_id_from_match_df.size(); ++i) {
    int teamId = team_api_id_from_match_df[i];
    int dateThisMatch = dateFromMatchDf[i];
    
    // Find the subset of vector of team_api_id_from_team_df which equals to this team_api_id in this iteration
    std::vector<int> rowNumber = std::vector<int> (); // rowNumber to record index
    for (int j = 0; j < (int) team_api_id_from_team_df.size(); ++j) {
      if (team_api_id_from_team_df[j] == teamId) {
        rowNumber.push_back(j);
      }
    }
    
    // Find the subset of date and score according to rowNumber
    std::vector<int> dateSubRange = std::vector<int> ();
    std::vector<int> scoreSubRange = std::vector<int> ();
    for (auto j = rowNumber.begin(); j != rowNumber.end(); ++j) {
      dateSubRange.push_back(dateFromTeamDf[*j]);
      scoreSubRange.push_back(scoreFromTeamDf[*j]);
    }
    
    // Find the date (from team df) which is the closest to the match date, and the score at this date
    int dateDif;
    int dateDifMin = INT_MAX;
    int dateClosest;
    int scoreThisDate;
    for (int j = 0; j < (int) dateSubRange.size(); ++j) {
      dateDif = abs(dateSubRange[j] - dateThisMatch);
      if (dateDif <= dateDifMin) {
        dateDifMin = dateDif;
        dateClosest = dateSubRange[j];
        scoreThisDate = scoreSubRange[j];
      }
    }
    
    // Save scoreThisDate to scoreOutput
    scoreOutput.push_back(scoreThisDate);
  }
  
  return (scoreOutput);
}

