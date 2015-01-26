/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package serialsentiment;

/**
 *
 * @author Sergio
 */
public class MovieRating
{
    public String movieID;
    public RatingScore[] ratings;

    public MovieRating(String id) {
        movieID = id;
        ratings = new RatingScore[5];

        for (int i = 0; i < 5; i++)
        {
            ratings[i] = new RatingScore();
            ratings[i].rating = i;
        }
    }
}
