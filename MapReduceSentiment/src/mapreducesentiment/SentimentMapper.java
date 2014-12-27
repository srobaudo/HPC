/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author camila
 */
public class SentimentMapper extends Mapper<NullWritable,BytesWritable,NullWritable,Text> {
    
       
	@Override
	public void map(NullWritable key, BytesWritable value, Context output) throws IOException, InterruptedException
	{
               try {
                   System.out.println("Entro al Map");
                   
                   byte[] arrayByte = value.copyBytes();
                   File archivo = new File("entradaMap.mdl");
                   try (FileOutputStream fos = new FileOutputStream(archivo)) {
                       fos.write(arrayByte);
                       fos.flush();
                   }   

                    Process process = new ProcessBuilder("mcell.exe", "entradaMap.mdl").start();
                   
                    //process.waitFor();
                    InputStream is = process.getInputStream();
                    InputStreamReader isr = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(isr);
                    String line;

                    System.out.println("Mcell is running");
                    String res = "";
                    while ((line = br.readLine()) != null) {
                        //escribir la salida de alguna forma
                        res = res.concat(line);
                        System.out.println(line);                        
                    }
                    
                    InputStream in = new FileInputStream(new File("joined_1.dat"));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    StringBuilder out = new StringBuilder();
                    String l;
                    while ((l = reader.readLine()) != null) {
                        out.append(l);
                        out.append("\n");
                    }
                    System.out.println(out.toString());
                    
                    output.write(key, new Text(out.toString()));
                    reader.close();
                    
                } catch (Exception ex) {
                    Logger.getLogger(SentimentMapper.class.getName()).log(Level.SEVERE, null, ex);
                }		
	}
        
        
}

