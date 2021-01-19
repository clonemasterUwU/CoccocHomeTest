import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

class TempFileBuffer {
    BufferedReader bf;
    String next;
    TempFileBuffer(File temp,Charset cs) throws IOException {
        bf = new BufferedReader(new FileReader(temp,cs));
    }
    boolean hasNext() throws IOException {
        next = bf.readLine();
        return next!=null;
    }
}
public class Main {
    private Charset charset = StandardCharsets.UTF_8;
    private boolean debug = false;
    final private String  prefix = "batch_";
    private File sortBatch(List<String> batch) throws IOException{
        Collections.sort(batch);
        File temp = File.createTempFile(prefix,".txt", new File(System.getProperty("user.dir")));
        try(BufferedWriter bfWriteTemp = new BufferedWriter(new FileWriter(temp,charset))){
            if(debug){
                System.out.printf("Batch - %d lines\n",batch.size());
                System.out.print("Writing to file: " + temp.getName());
            }
            for(String next:batch){
                bfWriteTemp.write(next);
                bfWriteTemp.append('\n');
            }
            if(debug){
                System.out.println("  Done.");
            }
            return temp;
        }

    }

    private List<File> splitAndSortFile(File input,long threshold) throws IOException {
        List<File> tempFile = new ArrayList<>();
        List<String> batchLine = new ArrayList<>();
        try (BufferedReader bufferIn = new BufferedReader(new FileReader(input,charset))) {
            String temp = bufferIn.readLine();
            long batchSize = 0;
            while (temp != null) {
                while (temp != null && batchSize + temp.getBytes().length < threshold) {
                    batchLine.add(temp);
                    batchSize+=temp.getBytes().length;
                    temp = bufferIn.readLine();
                }
                tempFile.add(sortBatch(batchLine));
                batchSize = 0;
                batchLine.clear();
            }
            if (debug) {
                System.out.println("Finish splitting.");
            }
            return tempFile;
        }
    }

    private void merge (File output, List<File> tempFile) throws IOException {
        List<TempFileBuffer> potentialNextLine = new ArrayList<>();

        try (BufferedWriter bufferOut = new BufferedWriter(new FileWriter(output,charset))){
            for(File temp:tempFile){
                potentialNextLine.add(new TempFileBuffer(temp,charset));
            }
            PriorityQueue<TempFileBuffer> pq = new PriorityQueue<>(Comparator.comparing(a -> a.next));
            for(TempFileBuffer tempLine:potentialNextLine){
                if(tempLine.hasNext()) pq.add(tempLine);
            }
            while(!pq.isEmpty()){
                TempFileBuffer next = pq.poll();
                bufferOut.write(next.next);
                bufferOut.append('\n');
                if(next.hasNext()) pq.add(next);
            }
        } finally {
            for(TempFileBuffer tempBuffer:potentialNextLine){
                if(tempBuffer!=null&&tempBuffer.bf!=null) tempBuffer.bf.close();
            }
            if(!debug){
                for(File temp:tempFile) temp.delete();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        File input = new File(args[0]);
        File output = new File(args[1]);
        long memoryLimit = Long.parseLong(args[2]);
        Main main = new Main();
        if(args.length>=4&&args[3].equals("-d")) main.debug=true;
        List<File> tempFile = main.splitAndSortFile(input,(long)(memoryLimit/2*0.8));
        main.merge(output,tempFile);
    }
}
