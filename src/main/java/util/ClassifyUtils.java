package util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.Hashtable;
import java.util.stream.Stream;

public final class ClassifyUtils {
    private ClassifyUtils(){}

    @SuppressWarnings("Duplicate")
    public static Hashtable<String,Long> mapKeyValueFromHDFS(Configuration configuration,String fileDirectoryKey) throws IOException {
        String fileDirectory = configuration.get(fileDirectoryKey);
        Path fileDirPath = new Path(fileDirectory);
        FileSystem fileSystem = fileDirPath.getFileSystem(configuration);
        FileStatus[] fileStatuses = fileSystem.listStatus(fileDirPath);
        Path filePath = Stream.of(fileStatuses).filter(e->e.getPath().getName().startsWith("part")).findFirst().get().getPath();
        Hashtable<String,Long> hashtable = new Hashtable<>();
        Text lineContent = new Text();
        int readSize = 0;
        String[] lineElements = null;
        try(FSDataInputStream inputStream = fileSystem.open(filePath);LineReader lineReader = new LineReader(inputStream)){
            while((readSize=lineReader.readLine(lineContent))>0){
                lineElements = StringUtils.splitByWholeSeparatorPreserveAllTokens(lineContent.toString(),"\t");
                hashtable.put(lineElements[0],Long.valueOf(lineElements[1]));
            }
        }
        return hashtable;
    }

    @SuppressWarnings("Duplicate")
    public static Hashtable<String,String> mapKeyStringValueFromHDFS(Configuration configuration,String fileDirectoryKey) throws IOException {
        String fileDirectory = configuration.get(fileDirectoryKey);
        Path fileDirPath = new Path(fileDirectory);
        FileSystem fileSystem = fileDirPath.getFileSystem(configuration);
        FileStatus[] fileStatuses = fileSystem.listStatus(fileDirPath);
        Path filePath = Stream.of(fileStatuses).filter(e->e.getPath().getName().startsWith("part")).findFirst().get().getPath();
        Hashtable<String,String> hashtable = new Hashtable<>();
        Text lineContent = new Text();
        int readSize = 0;
        String[] lineElements = null;
        try(FSDataInputStream inputStream = fileSystem.open(filePath);LineReader lineReader = new LineReader(inputStream)){
            while((readSize=lineReader.readLine(lineContent))>0){
                lineElements = StringUtils.splitByWholeSeparatorPreserveAllTokens(lineContent.toString(),"\t");
                hashtable.put(lineElements[0],lineElements[1]);
            }
        }
        return hashtable;
    }

}
