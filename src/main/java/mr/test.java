package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * Created by gaoqida on 15-7-14.
 */
public class test {

    public static void main(String[] args) throws IOException {
        System.out.print("start copy HDFS files");
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.1.129:18000"),new Configuration());

        InputStream inputStream = fileSystem.open(new Path("/test/mobile1"));

        File file = new File("/home/gaoqida/wahaha");
        file.createNewFile();
        OutputStream outputStream = new FileOutputStream(file);

        IOUtils.copyBytes(inputStream, outputStream, 4096, true);

        System.out.print("end receive.............");
    }

}
