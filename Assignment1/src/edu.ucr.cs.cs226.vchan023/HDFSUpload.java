package edu.ucr.cs.cs226.vchan023;
import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUpload {
    private static String src, destination;
    private static boolean status;
    public static void main(String args[]) throws IOException{
        src = args[0];
        destination = args[1];
        Path dst = new Path(destination);
        if((new File(src)).exists())
        {
            InputStream in = new BufferedInputStream(new FileInputStream(src));
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(destination), conf);
            try{
                if(fs.exists(dst)){
                    System.out.println("Destination already has the file");
                }else{
                    OutputStream out = fs.create(dst);
                    IOUtils.copyBytes(in, out, 4096, true);
                }
            }catch(Exception e){
                System.out.println("File transfer failed");
                System.err.println(e.toString());
            }
        }else{
            System.out.println("Source file doesnt exist");
        }
    }
}
