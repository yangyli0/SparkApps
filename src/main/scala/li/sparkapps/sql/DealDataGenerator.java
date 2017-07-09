package li.sparkapps.sql;

import java.util.Random;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.io.IOException;

/**
 * Created by lee on 7/8/17.
 */
public class DealDataGenerator {
    private static final String filePath = "./sqldata/sample_deal_data.txt";
    private static final int[] PRODUCT_ID_ARR = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    private static final int MAX_PRICE = 2000;
    private static final int MIN_PRICE = 10;
    private int numOfUser;
    private int numOfDeal;
    public DealDataGenerator(int numOfUser, int numOfDeal) {
        this.numOfUser = numOfUser;
        this.numOfDeal = numOfDeal;
    }

    public void generateDealData() {
        String lineSeparator = System.getProperty("line.separator");
        ByteBuffer byteBuf = ByteBuffer.allocate(100);
        RandomAccessFile raf = null;
        Random rand = new Random();
        try {
            raf = new RandomAccessFile(filePath, "rw");
            FileChannel fc = raf.getChannel();
            for (int i = 1; i <= numOfDeal; i++) {
                // 日期
                int year = rand.nextInt(18) + 2000;
                int month = rand.nextInt(12) + 1;
                int day = rand.nextInt(28) + 1;
                String dealDate = year + "-" + month + "-" + day;
                //　产品Id
                int productIdIndex = rand.nextInt(PRODUCT_ID_ARR.length);
                int productId = PRODUCT_ID_ARR[productIdIndex];
                // 价格
                int price = rand.nextInt(MAX_PRICE-MIN_PRICE+1) + MIN_PRICE;
                // 用户Id
                int userId = rand.nextInt(numOfUser) + 1;
                String dealRecord = i + " " + dealDate + " " + productId + " " +
                        price + " " + userId + lineSeparator;
                byteBuf.put(dealRecord.getBytes());
                byteBuf.flip();
                fc.write(byteBuf);
                byteBuf.clear();
            }
             System.out.println("Deal data generated successfully");
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
        if (args.length < 2) {
            System.out.println("Please specify <numOfUser> and <numOfDeal>");
            System.exit(1);
        }
        int numOfUser = Integer.parseInt(args[0]);
        int numOfDeal = Integer.parseInt(args[1]);
        DealDataGenerator generator = new DealDataGenerator(numOfUser, numOfDeal);
        generator.generateDealData();
    }

}
