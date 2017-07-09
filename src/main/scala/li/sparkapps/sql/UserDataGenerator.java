package li.sparkapps.sql;




import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.util.Random;

/**
 * Created by lee on 7/8/17.
 */
public class UserDataGenerator {
    private static final String FILE_PATH = "./sqldata/sample_user_data.txt";
    private static final String[] ROLE_ID_ARR = {"ROLE_1", "ROLE_2", "ROLE_3", "ROLE_4", "ROLE_5"};
    private static final String[] REGION_ID_ARR = {"REG_1", "REG_2", "REG_3", "REG_4", "REG_5"};
    private static final int MAX_USER_AGE = 60;
    private static final int MIN_USER_AGE = 20;
    private int numOfUser = 0;

    public UserDataGenerator (int numOfUser) {
        this.numOfUser = numOfUser;
    }

    public void generateUserData() {
        String  lineSeparator = System.getProperty("line.separator");
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(FILE_PATH, "rw");
            FileChannel fc = raf.getChannel();
            ByteBuffer byteBuf = ByteBuffer.allocate(100);
            Random rand = new Random();
            for (int i = 1; i <= numOfUser; i++) {
                // 性别
                String gender = rand.nextInt() % 2 == 0 ? "F": "M";
                // 年龄
                int age = rand.nextInt(MAX_USER_AGE - MIN_USER_AGE + 1) + MIN_USER_AGE;

                // 注册日期　
                int year = rand.nextInt(18) + 2000;
                int month = rand.nextInt(12) + 1;
                int day = rand.nextInt(28) + 1;
                String registerDate = year + "-" + month + "-" + day;
                // 角色
                int roleIndex = rand.nextInt(ROLE_ID_ARR.length);
                String role = ROLE_ID_ARR[roleIndex];
                // 地域
                int regionIndex = rand.nextInt(REGION_ID_ARR.length);
                String region = REGION_ID_ARR[regionIndex];
                String record = i + " " + gender + " " + age + " " + registerDate + " " +
                        role + " " + region + lineSeparator;
                byteBuf.put(record.getBytes());
                byteBuf.flip();
                fc.write(byteBuf);
                byteBuf.clear();

            }
            System.out.println("User data generated successfully");
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
            System.out.println("Please specify <numOfRecords>.");
            System.exit(1);
        }
        int numOfRecords = Integer.parseInt(args[0]);
        UserDataGenerator generator = new UserDataGenerator(numOfRecords);
        generator.generateUserData();
    }
}
