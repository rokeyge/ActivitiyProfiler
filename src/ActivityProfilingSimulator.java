import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ActivityProfilingSimulator {

    private final static String LOCATION_PATH = "/home/rokey/Work/workspace/activityProfiler/locations/";

//    private final static String FILE_NAME = "locations-ch1.csv";
//    private final static String FILE_NAME = "locations.csv";
    private final static String FILE_NAME = "locationsN.csv";

    private static ActivityProfiler mProfiler;

    private static int mLastCid;

    private static long mLastTime;

    private static int mCidChangeCounter = 0;

    public static void main(String[] args) {

        mProfiler = new ActivityProfiler();

        File locationFile = new File(LOCATION_PATH + FILE_NAME);
        if (!locationFile.exists()) {
            return;
        }

        FileReader in;
        try {
            in = new FileReader(locationFile);
            BufferedReader reader = new BufferedReader(in);
            String line;
            boolean firstline = true;
            while ((line = reader.readLine()) != null) {
                if (firstline) {

                    // fix the bug that the first cid does not have stat
                    // CidStats cStat = new CidStats();
                    // cStat.setCid(-1);
                    // mCidStats.put(-1, cStat);

                    firstline = false;
                    continue;
                }

                parseline(line);
            }

            in.close();

            mProfiler.shutdown(mLastTime);
//            System.out.println("pingpong: " + mPingpongEvents);
//            System.out.println("short pingpong: " + mShortPingpongCounter);
//            System.out.println("cid change events: " + mCidChangeCounter);

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void parseline(String line) {
        String[] splits = line.split(";");
        if (splits.length != 10) {
            System.out.println("Incorrect line " + line);
        }

        try {
            int sequence = Integer.parseInt(splits[0]);

            long epochTime = Long.parseLong(splits[1]);

            Float gpsLong = Float.parseFloat(splits[2]);

            Float gpsLat = Float.parseFloat(splits[3]);

            float netLong = Float.parseFloat(splits[4]);

            float netLat = Float.parseFloat(splits[5]);

            int cid = Integer.parseInt(splits[6]);

            int lac = Integer.parseInt(splits[7]);

            int networkType = Integer.parseInt(splits[8]);

            int signalStr = Integer.parseInt(splits[9]);

            if (cid == -1) {
                return;
            }

            // Process the cooridnates
            if (gpsLong == -999999 || gpsLat == -999999) {
                gpsLong = null;
                gpsLat = null;
            }

            // If cid has changed, raise an event
            if (cid != mLastCid) {
                 mProfiler.newCidEvent(cid, epochTime, gpsLat, gpsLong);
//                pingpongTest(cid, epochTime);
                mCidChangeCounter++;
            }

            mLastCid = cid;
            mLastTime = epochTime;

        } catch (NumberFormatException exp) {
            System.err.print("Failed to parse the line " + line);
        }
    }

    static int CIR_BUF_SIZE = 2;

    static int[] visitedCidBuffer = new int[CIR_BUF_SIZE];

    static long[] connectDurationBuffer = new long[CIR_BUF_SIZE];

    private static int mPingpongEvents = 0;

    private static long mLastCidChangeTime;

    private static int mShortPingpongCounter = 0;

    private static final long PING_PONG_THRESHOLD = 15 * 60 * 1000;

    private static void pingpongTest(int cid, long time) {

        long duration = time - mLastCidChangeTime;
        mLastCidChangeTime = time;

        for (int i = 0; i < CIR_BUF_SIZE - 1; i++) {
            visitedCidBuffer[i] = visitedCidBuffer[i + 1];
            connectDurationBuffer[i] = connectDurationBuffer[i + 1];
        }
        visitedCidBuffer[CIR_BUF_SIZE - 1] = mLastCid;
        connectDurationBuffer[CIR_BUF_SIZE - 1] = duration;

        for (int i = CIR_BUF_SIZE - 1; i > -1; i--) {
            if (visitedCidBuffer[i] == cid) {
                mPingpongEvents++;
                long elapsedTime = 0;
                for (int j = i; j < CIR_BUF_SIZE; j++) {
                    elapsedTime += connectDurationBuffer[j];
                }

                if (elapsedTime < PING_PONG_THRESHOLD) {
                    mShortPingpongCounter++;
                }
            }
        }
    }
}
