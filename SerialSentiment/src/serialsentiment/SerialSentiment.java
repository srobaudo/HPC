/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package serialsentiment;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 *
 * @author Sergio
 */
public class SerialSentiment {

    private static final String pathToCorpus = "C:\\Users\\Sergio\\Documents\\GitHub\\HPC\\SerialSentiment\\movies.txt";
    
    public static final Pattern pattern = Pattern.compile("product\\/productId\\:\\s+([a-zA-Z0-9]+).*review\\/score\\:\\s+([\\d]+)\\..*review\\/text\\:\\s+(.*)", 
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
    
    private static final HashMap<String, MovieRating> results = new HashMap<>();
    
    public static void main(String[] args) 
    {
        try (Stream<String> stream = Files.lines(Paths.get(pathToCorpus), Charset.defaultCharset()))
        {
            Iterator<String> lines = stream.iterator();
            while (lines.hasNext())
            {
                String nextToProcess = GetNextComment(lines);
                ParseComment(nextToProcess);
            }
        } catch (IOException ex) {
            Logger.getLogger(SerialSentiment.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static String GetNextComment(Iterator<String> lines) 
    {
        boolean previousWasLineBreak = false;
        boolean currentIsLineBreak;
        String currentLine;
        String nextToProcess = "";
        while (lines.hasNext())
        {
            currentLine = lines.next();
            nextToProcess += currentLine;
            
            currentIsLineBreak = System.lineSeparator().equals(currentLine);
            if (previousWasLineBreak && currentIsLineBreak)
            {
                break;
            }
            previousWasLineBreak = currentIsLineBreak;
        }
        return nextToProcess;
    }

    private static void ParseComment(String nextToProcess)
    {
        Matcher matcher = pattern.matcher(nextToProcess);
        if (matcher.find()) {
            int fieldCount = matcher.groupCount();
            String[] fields = new String[fieldCount];
            
            for (int i = 0; i < fieldCount; i++) {
                fields[i] = matcher.group(i + 1);
            }
            
            String movieID = fields[1];
            Long rating = Long.parseLong(fields[2]);
            String comment = fields[3];
            
            Long score = SentimentAnalysis(comment);
            
            if (!results.containsKey(movieID))
            {
                results.put(movieID, new MovieRating(movieID));
            }
            results.get(movieID).ratings[rating.intValue() - 1].scores.add(score);
        }
    }
    
    private static Long SentimentAnalysis(String comment)
    {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution 
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(comment);

        // run all Annotators on this text
        pipeline.annotate(document);

        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        int result = 0;
        int count = 0;
        for (CoreMap sentence : sentences) 
        {
            String sentimentStr = sentence.get(SentimentCoreAnnotations.ClassName.class);

            Tree sentimentTree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(sentimentTree);

            System.out.println("(" + sentiment + ")" + sentimentStr + "\t->\t " + sentence.toString());

            result += sentiment;
            count++;
        }

        double average = count > 0 ? (double) result / (double) count : 0;
        Long redondeo = Math.round(average);
        System.out.println("Resultado del comentario: " + redondeo + " (" + average + ")");
        
        return redondeo;
    }
}
