/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author camila
 */
public class SentimentMapper extends Mapper<SentimentKeyWritableComparable, Text, SentimentKeyWritableComparable, LongWritable> {

    @Override
    public void map(SentimentKeyWritableComparable key, Text value, Context output) throws IOException, InterruptedException {
        try {
            System.out.println("Entro al Map");

            // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution 
            Properties props = new Properties();
            props.put("annotators", "tokenize, ssplit, parse, sentiment");
            StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

            // create an empty Annotation just with the given text
            Annotation document = new Annotation(value.toString());

            // run all Annotators on this text
            pipeline.annotate(document);

       // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(SentencesAnnotation.class);

            int result = 0;
            int count = 0;
            for (CoreMap sentence : sentences) {
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
            output.write(key, new LongWritable(redondeo));

        } catch (Exception ex) {
            Logger.getLogger(SentimentMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
