package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by gaoqida on 15-7-6.
 */
public class Controller {

    public static void main(String[] args) throws IOException {
        try {
            String dsf = "hdfs://localhost:9000/tmp/visit.log";
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(dsf),conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(dsf));
            FileOutputStream fileOutputStream = new FileOutputStream("/home/gaoqida/1");

            byte[] ioBuffer = new byte[1024];
            int readLen = hdfsInStream.read(ioBuffer);
            while(readLen!=-1)
            {
                readLen = hdfsInStream.read(ioBuffer);
                fileOutputStream.write(hdfsInStream.read(ioBuffer));
            }
            hdfsInStream.close();
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
