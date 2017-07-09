package li.sparkapps.sql;

import java.util.Random;
import java.nio.channels.FileChannel;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Created by lee on 7/8/17.
 */
public class PopulationDataGenerator {
    private static final String FILE_PATH = "./sqldata/population.txt";
    private static final int  MAX_HEIGHT = 233;
    private static final int MIN_HEIGHT = 152;
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private int numOfRecord;
    public PopulationDataGenerator(int numOfRecord) {
        this.numOfRecord = numOfRecord;
    }

    public void generate() {
        Random rand = new Random();
        RandomAccessFile raf = null;
        ByteBuffer byteBuf = ByteBuffer.allocate(15);
        try {
            raf = new RandomAccessFile(FILE_PATH, "rw");
            FileChannel fc = raf.getChannel();
            for (int i = 1; i <= numOfRecord; i++) {
                String gender = (i % 2 == 0) ? "F" : "M";
                int height = rand.nextInt(MAX_HEIGHT - MIN_HEIGHT + 1) + MIN_HEIGHT;
                String record = i + " " + gender + " " + height + LINE_SEPARATOR;
                byteBuf.put(record.getBytes());
                byteBuf.flip();
                fc.write(byteBuf);
                byteBuf.clear();
            }
            System.out.println("Population data successfully generated");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {}
            }
        }

    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please specify the <numOfRecord>");
            System.exit(1);
        }
        int numOfRecords = Integer.parseInt(args[0]);
        PopulationDataGenerator generator = new PopulationDataGenerator(numOfRecords);
        generator.generate();

    }
}
